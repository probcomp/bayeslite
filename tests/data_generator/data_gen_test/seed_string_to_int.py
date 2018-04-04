def hash_32_unsigned(seed_string):
    return hash(seed_string) & 0xFFFFFFFF
