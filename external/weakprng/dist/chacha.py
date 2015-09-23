# ChaCha core: family of PRFs from 128-bit inputs to 512-bit outputs
# with 256-bit key, defined in
#
#       Daniel J. Bernstein, `ChaCha, a variant of Salsa20', Workshop
#       Record of SASC 2008: The State of the Art of Stream Ciphers.
#       Document ID: 4027b5256e17b9796842e6d0f68b0b5e
#       http://cr.yp.to/chacha/chacha-20080128.pdf

# Copyright (c) 2015 Taylor R. Campbell
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
# OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.

###############################################################################
### WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING ###
###############################################################################
###                                                                         ###
###   This code is NOT constant-time -- using it makes you vulnerable to    ###
###   timing side channels.  Do not use this for cryptographic purposes.    ###
###                                                                         ###
###############################################################################
### WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING ###
###############################################################################

# Even on CPUs that do provide constant-time 32-bit bitwise operations
# and addition, CPython has no way to reliably take advantage of them.
# For example, on 32-bit CPUs, CPython uses a different representation
# of integers < 2^31 and integers >= 2^31, with significantly different
# timings for arithmetic.
#
# This is unlike C, in which carefully written programs are unlikely to
# have timing side channels even though nothing prohibits compilers
# from compiling them to run in variable-time (and to be safe you
# should check the assembly and confirm that the microarchitecture
# executing the instructions will execute them in constant time and so
# on).  In contrast, Python practically guarantees that all arithmetic
# *will* have timing side channels.

import struct

def quarterround(x, a, b, c, d):
    # a += b; d ^= a; d <<<= 16
    x[a] = (x[a] + x[b]) & 0xffffffff
    x[d] ^= x[a]
    x[d] = ((x[d] & 0xffff) << 16) | (x[d] >> 16)
    # c += d; b ^= c; b <<<= 12
    x[c] = (x[c] + x[d]) & 0xffffffff
    x[b] ^= x[c]
    x[b] = ((x[b] & 0xfffff) << 12) | (x[b] >> 20)
    # a += b; d ^= a; d <<<= 8
    x[a] = (x[a] + x[b]) & 0xffffffff
    x[d] ^= x[a]
    x[d] = ((x[d] & 0xffffff) << 8) | (x[d] >> 24)
    # c += d; b ^= c; b <<<= 7
    x[c] = (x[c] + x[d]) & 0xffffffff
    x[b] ^= x[c]
    x[b] = ((x[b] & 0x1ffffff) << 7) | (x[b] >> 25)

def quarterround_selftest():
    # From https://tools.ietf.org/html/draft-nir-cfrg-chacha20-poly1305-00
    x = [0x11111111, 0x01020304, 0x9b8d6f43, 0x01234567]
    quarterround(x, 0, 1, 2, 3)
    if x != [0xea2a92f4, 0xcb1cf8ce, 0x4581472e, 0x5881c4bb]:
        raise Exception('chacha quarterround 0 1 2 3 self-test failed')
    x = [
        0x879531e0, 0xc5ecf37d, 0x516461b1, 0xc9a62f8a,
        0x44c20ef3, 0x3390af7f, 0xd9fc690b, 0x2a5f714c,
        0x53372767, 0xb00a5631, 0x974c541a, 0x359e9963,
        0x5c971061, 0x3d631689, 0x2098d9d6, 0x91dbd320,
    ]
    quarterround(x, 2, 7, 8, 13)
    if x != [
            0x879531e0, 0xc5ecf37d, 0xbdb886dc, 0xc9a62f8a,
            0x44c20ef3, 0x3390af7f, 0xd9fc690b, 0xcfacafd2,
            0xe46bea80, 0xb00a5631, 0x974c541a, 0x359e9963,
            0x5c971061, 0xccc07c79, 0x2098d9d6, 0x91dbd320,
    ]:
        raise Exception('chacha quarterround 2 7 8 13 self-test failed')
quarterround_selftest()

def core(n, o, i, k, c):
    assert n%2 == 0
    x = [
        c[0], c[1], c[2], c[3], k[0], k[1], k[2], k[3],
        k[4], k[5], k[6], k[7], i[0], i[1], i[2], i[3],
    ]
    y = x[0:16]                 # allow o = k
    for r in range(n/2):
        quarterround(y, 0, 4, 8,12)
        quarterround(y, 1, 5, 9,13)
        quarterround(y, 2, 6,10,14)
        quarterround(y, 3, 7,11,15)
        quarterround(y, 0, 5,10,15)
        quarterround(y, 1, 6,11,12)
        quarterround(y, 2, 7, 8,13)
        quarterround(y, 3, 4, 9,14)
    o[0] = (x[0] + y[0]) & 0xffffffff
    o[1] = (x[1] + y[1]) & 0xffffffff
    o[2] = (x[2] + y[2]) & 0xffffffff
    o[3] = (x[3] + y[3]) & 0xffffffff
    o[4] = (x[4] + y[4]) & 0xffffffff
    o[5] = (x[5] + y[5]) & 0xffffffff
    o[6] = (x[6] + y[6]) & 0xffffffff
    o[7] = (x[7] + y[7]) & 0xffffffff
    o[8] = (x[8] + y[8]) & 0xffffffff
    o[9] = (x[9] + y[9]) & 0xffffffff
    o[10] = (x[10] + y[10]) & 0xffffffff
    o[11] = (x[11] + y[11]) & 0xffffffff
    o[12] = (x[12] + y[12]) & 0xffffffff
    o[13] = (x[13] + y[13]) & 0xffffffff
    o[14] = (x[14] + y[14]) & 0xffffffff
    o[15] = (x[15] + y[15]) & 0xffffffff

const32 = struct.unpack('<IIII', 'expand 32-byte k')
if const32 != (0x61707865, 0x3320646e, 0x79622d32, 0x6b206574):
    raise Exception('chacha constant32 self-test failed')

def core_selftest():
    # From http://tools.ietf.org/html/draft-strombergson-chacha-test-vectors-00
    o = [0] * 16
    i = [0] * 4
    k = [0] * 8
    core(8, o, i, k, const32)
    if o != list(struct.unpack('<IIIIIIIIIIIIIIII', bytes(bytearray([
            0x3e, 0x00, 0xef, 0x2f, 0x89, 0x5f, 0x40, 0xd6,
            0x7f, 0x5b, 0xb8, 0xe8, 0x1f, 0x09, 0xa5, 0xa1,
            0x2c, 0x84, 0x0e, 0xc3, 0xce, 0x9a, 0x7f, 0x3b,
            0x18, 0x1b, 0xe1, 0x88, 0xef, 0x71, 0x1a, 0x1e,
            0x98, 0x4c, 0xe1, 0x72, 0xb9, 0x21, 0x6f, 0x41,
            0x9f, 0x44, 0x53, 0x67, 0x45, 0x6d, 0x56, 0x19,
            0x31, 0x4a, 0x42, 0xa3, 0xda, 0x86, 0xb0, 0x01,
            0x38, 0x7b, 0xfd, 0xb8, 0x0e, 0x0c, 0xfe, 0x42,
    ])))):
        raise Exception('chacha8 core self-test failed')
    core(20, o, i, k, const32)
    if o != list(struct.unpack('<IIIIIIIIIIIIIIII', bytes(bytearray([
            0x76, 0xb8, 0xe0, 0xad, 0xa0, 0xf1, 0x3d, 0x90,
            0x40, 0x5d, 0x6a, 0xe5, 0x53, 0x86, 0xbd, 0x28,
            0xbd, 0xd2, 0x19, 0xb8, 0xa0, 0x8d, 0xed, 0x1a,
            0xa8, 0x36, 0xef, 0xcc, 0x8b, 0x77, 0x0d, 0xc7,
            0xda, 0x41, 0x59, 0x7c, 0x51, 0x57, 0x48, 0x8d,
            0x77, 0x24, 0xe0, 0x3f, 0xb8, 0xd8, 0x4a, 0x37,
            0x6a, 0x43, 0xb8, 0xf4, 0x15, 0x18, 0xa1, 0x1c,
            0xc3, 0x87, 0xb6, 0x69, 0xb2, 0xee, 0x65, 0x86,
    ])))):
        raise Exception('chacha20 core self-test failed')
    # From https://tools.ietf.org/html/draft-nir-cfrg-chacha20-poly1305-00
    i = struct.unpack('<IIII', bytes(bytearray([
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,
        0x00, 0x00, 0x00, 0x4a, 0x00, 0x00, 0x00, 0x00,
    ])))
    k = struct.unpack('<IIIIIIII', bytes(bytearray([
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
        0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
    ])))
    core(20, o, i, k, const32)
    if o != [
       0xe4e7f110, 0x15593bd1, 0x1fdd0f50, 0xc47120a3,
       0xc7f4d1c7, 0x0368c033, 0x9aaa2204, 0x4e6cd4c3,
       0x466482d2, 0x09aa9f07, 0x05d7c214, 0xa2028bd9,
       0xd19c12b5, 0xb94e16de, 0xe883d0cb, 0x4e3c50a2,
    ]:
        raise Exception('chacha20 core self-test failed')
core_selftest()
