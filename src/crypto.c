#include "td_crypto.h"

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <string.h>

static uint64_t td_crypto_profile_begin(td_crypto_profile_t *profile) {
    return profile != NULL ? td_now_ns() : 0;
}

static void td_crypto_profile_end(td_crypto_profile_t *profile, uint64_t start_ns, uint64_t *field) {
    if (profile != NULL && field != NULL && start_ns != 0) {
        *field += td_now_ns() - start_ns;
    }
}

static void td_sha256_two_parts(const unsigned char *a, size_t a_len, const unsigned char *b, size_t b_len, unsigned char out[SHA256_DIGEST_LENGTH]) {
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, a, a_len);
    SHA256_Update(&ctx, b, b_len);
    SHA256_Final(out, &ctx);
}

static int td_crypto_mac(const td_crypto_ctx_t *ctx, const td_slot_t *slot, unsigned char out[32], uint64_t *latency_ns, td_crypto_profile_t *profile) {
    unsigned int out_len = 0;
    HMAC_CTX *hctx;
    uint64_t total_start_ns = latency_ns != NULL ? td_now_ns() : 0;
    uint64_t start_ns;

    start_ns = td_crypto_profile_begin(profile);
    hctx = HMAC_CTX_new();
    if (hctx == NULL) {
        return -1;
    }
    if (HMAC_Init_ex(hctx, ctx->mac_key, sizeof(ctx->mac_key), EVP_sha256(), NULL) != 1) {
        HMAC_CTX_free(hctx);
        return -1;
    }
    td_crypto_profile_end(profile, start_ns, profile != NULL ? &profile->mac_setup_ns : NULL);

    start_ns = td_crypto_profile_begin(profile);
    if (HMAC_Update(hctx, (const unsigned char *)&slot->header.key_hash, sizeof(slot->header.key_hash)) != 1 ||
        HMAC_Update(hctx, (const unsigned char *)&slot->header.visible_epoch, sizeof(slot->header.visible_epoch)) != 1 ||
        HMAC_Update(hctx, (const unsigned char *)&slot->header.tie_breaker, sizeof(slot->header.tie_breaker)) != 1 ||
        HMAC_Update(hctx, (const unsigned char *)&slot->header.flags, sizeof(slot->header.flags)) != 1 ||
        HMAC_Update(hctx, (const unsigned char *)&slot->header.value_len, sizeof(slot->header.value_len)) != 1 ||
        HMAC_Update(hctx, slot->header.iv, sizeof(slot->header.iv)) != 1 ||
        HMAC_Update(hctx, slot->ciphertext, slot->header.value_len) != 1) {
        HMAC_CTX_free(hctx);
        return -1;
    }
    td_crypto_profile_end(profile, start_ns, profile != NULL ? &profile->mac_body_ns : NULL);

    start_ns = td_crypto_profile_begin(profile);
    if (HMAC_Final(hctx, out, &out_len) != 1) {
        HMAC_CTX_free(hctx);
        return -1;
    }
    HMAC_CTX_free(hctx);
    td_crypto_profile_end(profile, start_ns, profile != NULL ? &profile->mac_finalize_ns : NULL);
    if (latency_ns != NULL && total_start_ns != 0) {
        *latency_ns += td_now_ns() - total_start_ns;
    }
    return out_len == 32 ? 0 : -1;
}

int td_crypto_init(td_crypto_ctx_t *ctx, const char *hex_key, char *err, size_t err_len) {
    unsigned char raw[TD_KEY_MATERIAL_BYTES];
    unsigned char enc_seed[SHA256_DIGEST_LENGTH];
    unsigned char mac_seed[SHA256_DIGEST_LENGTH];
    static const unsigned char enc_label[] = "dist-td-enc";
    static const unsigned char mac_label[] = "dist-td-mac";

    memset(ctx, 0, sizeof(*ctx));
    if (td_hex_to_bytes(hex_key, raw, sizeof(raw)) != 0) {
        td_format_error(err, err_len, "encryption_key_hex must be exactly %d hex chars", TD_KEY_MATERIAL_BYTES * 2);
        return -1;
    }
    td_sha256_two_parts(raw, sizeof(raw), enc_label, sizeof(enc_label) - 1, enc_seed);
    td_sha256_two_parts(raw, sizeof(raw), mac_label, sizeof(mac_label) - 1, mac_seed);
    memcpy(ctx->enc_key, enc_seed, sizeof(ctx->enc_key));
    memcpy(ctx->mac_key, mac_seed, sizeof(ctx->mac_key));
    return 0;
}

uint64_t td_crypto_tie_breaker(const char *logical_key, const unsigned char *value, size_t value_len, uint64_t epoch) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, logical_key, strlen(logical_key));
    SHA256_Update(&ctx, value, value_len);
    SHA256_Update(&ctx, &epoch, sizeof(epoch));
    SHA256_Final(digest, &ctx);
    return ((uint64_t)digest[0] << 56) |
           ((uint64_t)digest[1] << 48) |
           ((uint64_t)digest[2] << 40) |
           ((uint64_t)digest[3] << 32) |
           ((uint64_t)digest[4] << 24) |
           ((uint64_t)digest[5] << 16) |
           ((uint64_t)digest[6] << 8) |
           (uint64_t)digest[7];
}

int td_crypto_make_slot_profiled(
    const td_crypto_ctx_t *ctx,
    const char *logical_key,
    const unsigned char *value,
    size_t value_len,
    uint32_t flags,
    uint64_t epoch,
    td_slot_t *slot,
    td_crypto_profile_t *profile) {
    EVP_CIPHER_CTX *cipher;
    unsigned char iv_seed[SHA256_DIGEST_LENGTH];
    int out_len = 0;
    int tmp_len = 0;
    uint64_t start_ns;

    if (value_len > TD_MAX_VALUE_SIZE) {
        return -1;
    }

    memset(slot, 0, sizeof(*slot));
    slot->header.guard_epoch = epoch;
    slot->header.visible_epoch = epoch;
    start_ns = td_crypto_profile_begin(profile);
    slot->header.key_hash = td_hash64_string(logical_key);
    td_crypto_profile_end(profile, start_ns, profile != NULL ? &profile->slot_hash_ns : NULL);
    start_ns = td_crypto_profile_begin(profile);
    slot->header.tie_breaker = td_crypto_tie_breaker(logical_key, value, value_len, epoch);
    td_crypto_profile_end(profile, start_ns, profile != NULL ? &profile->tie_breaker_ns : NULL);
    slot->header.flags = flags;
    slot->header.value_len = (uint32_t)value_len;

    start_ns = td_crypto_profile_begin(profile);
    td_sha256_two_parts((const unsigned char *)logical_key, strlen(logical_key), (const unsigned char *)&epoch, sizeof(epoch), iv_seed);
    td_crypto_profile_end(profile, start_ns, profile != NULL ? &profile->iv_ns : NULL);
    memcpy(slot->header.iv, iv_seed, sizeof(slot->header.iv));

    start_ns = td_crypto_profile_begin(profile);
    cipher = EVP_CIPHER_CTX_new();
    if (cipher == NULL) {
        return -1;
    }
    if (EVP_EncryptInit_ex(cipher, EVP_aes_256_ctr(), NULL, ctx->enc_key, slot->header.iv) != 1) {
        EVP_CIPHER_CTX_free(cipher);
        return -1;
    }
    td_crypto_profile_end(profile, start_ns, profile != NULL ? &profile->encrypt_setup_ns : NULL);
    start_ns = td_crypto_profile_begin(profile);
    if (EVP_EncryptUpdate(cipher, slot->ciphertext, &out_len, value, (int)value_len) != 1 ||
        EVP_EncryptFinal_ex(cipher, slot->ciphertext + out_len, &tmp_len) != 1) {
        EVP_CIPHER_CTX_free(cipher);
        return -1;
    }
    td_crypto_profile_end(profile, start_ns, profile != NULL ? &profile->encrypt_ns : NULL);
    EVP_CIPHER_CTX_free(cipher);
    slot->header.value_len = (uint32_t)(out_len + tmp_len);

    if (td_crypto_mac(ctx, slot, slot->header.mac, profile != NULL ? &profile->mac_ns : NULL, profile) != 0) {
        return -1;
    }
    return 0;
}

int td_crypto_make_slot(
    const td_crypto_ctx_t *ctx,
    const char *logical_key,
    const unsigned char *value,
    size_t value_len,
    uint32_t flags,
    uint64_t epoch,
    td_slot_t *slot) {
    return td_crypto_make_slot_profiled(ctx, logical_key, value, value_len, flags, epoch, slot, NULL);
}

int td_crypto_decode_slot_profiled(
    const td_crypto_ctx_t *ctx,
    const char *logical_key,
    const td_slot_t *slot,
    unsigned char *plaintext,
    size_t *plaintext_len,
    td_crypto_profile_t *profile) {
    EVP_CIPHER_CTX *cipher;
    unsigned char mac[32];
    int out_len = 0;
    int tmp_len = 0;
    uint64_t start_ns;

    (void)logical_key;

    if (slot->header.value_len > TD_MAX_VALUE_SIZE) {
        return -1;
    }
    if (slot->header.guard_epoch != slot->header.visible_epoch) {
        return -1;
    }
    if (td_crypto_mac(ctx, slot, mac, profile != NULL ? &profile->verify_mac_ns : NULL, profile) != 0) {
        return -1;
    }
    if (memcmp(mac, slot->header.mac, sizeof(mac)) != 0) {
        return -1;
    }
    if ((slot->header.flags & TD_SLOT_FLAG_VALID) == 0) {
        return -1;
    }

    start_ns = td_crypto_profile_begin(profile);
    cipher = EVP_CIPHER_CTX_new();
    if (cipher == NULL) {
        return -1;
    }
    if (EVP_DecryptInit_ex(cipher, EVP_aes_256_ctr(), NULL, ctx->enc_key, slot->header.iv) != 1) {
        EVP_CIPHER_CTX_free(cipher);
        return -1;
    }
    td_crypto_profile_end(profile, start_ns, profile != NULL ? &profile->decrypt_setup_ns : NULL);
    start_ns = td_crypto_profile_begin(profile);
    if (EVP_DecryptUpdate(cipher, plaintext, &out_len, slot->ciphertext, (int)slot->header.value_len) != 1 ||
        EVP_DecryptFinal_ex(cipher, plaintext + out_len, &tmp_len) != 1) {
        EVP_CIPHER_CTX_free(cipher);
        return -1;
    }
    td_crypto_profile_end(profile, start_ns, profile != NULL ? &profile->decrypt_ns : NULL);
    EVP_CIPHER_CTX_free(cipher);
    *plaintext_len = (size_t)(out_len + tmp_len);
    return 0;
}

int td_crypto_decode_slot(
    const td_crypto_ctx_t *ctx,
    const char *logical_key,
    const td_slot_t *slot,
    unsigned char *plaintext,
    size_t *plaintext_len) {
    return td_crypto_decode_slot_profiled(ctx, logical_key, slot, plaintext, plaintext_len, NULL);
}
