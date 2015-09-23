# Pseudorandom number generator using ChaCha8.  This does NOT provide
# key erasure / forward secrecy / backtracking resistance.

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

import chacha
import os
import struct
import threading

class WeakPRNG(object):
    const32 = chacha.const32
    zero = [0,0,0,0, 0,0,0,0]

    def __init__(self, seed):
        self.key = struct.unpack('<IIIIIIII', seed)
        self.ctr = [0,0,0,0]
        self.buf = [0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0]

    def weakrandom32(self):
        # We abuse buf[15] as a decrementing counter for the number
        # 32-bit words remaining to be yielded, since we refill the
        # buffer only when we need at least one word.
        buf = self.buf
        if buf[15] == 0:
            ctr = self.ctr
            chacha.core(8, buf, ctr, self.key, self.const32)
            t = 1
            for i in range(4):  # 4 is overkill
                t = ctr[i] + t
                ctr[i] = t & 0xffffffff
                t >>= 32
            r = buf[15]
            buf[15] = 15
            return r
        buf[15] -= 1
        return buf[buf[15]]

    def weakrandom64(self):
        rh = self.weakrandom32()
        rl = self.weakrandom32()
        return (rh << 32) | rl

    def weakrandom_uniform(self, n):
        assert 0 < n
        nbits = n.bit_length()
        nwords = (nbits + 31)/32
        l = ((1 << nbits) - n) % n
        while True:
            r = 0
            for i in range(nwords):
                r <<= 32
                r |= self.weakrandom32()
            if r < l:
                continue
            return (r % n)

    def weakrandom_bytearray(self, buf, start, end):
        assert end <= len(buf)
        assert start <= end
        nbytes = end - start
        if nbytes < 128:
            nwords = nbytes/4
            for i in range(nwords):
                buf[start + 4*i : start + 4*(i + 1)] = \
                    struct.pack('<I', self.weakrandom32())
            nextra = nbytes - 4*nwords
            if 0 < nextra:
                buf[start + 4*nwords : start + 4*nwords + nextra] = \
                    struct.pack('<I', self.weakrandom32())[0 : nextra]
        else:
            subkey = [self.weakrandom32() for i in range(8)]
            const32 = self.const32
            ctr = [0,0,0,0]
            out = [0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0]
            nblocks = nbytes/64
            for i in range(nblocks):
                chacha.core(8, out, ctr, subkey, const32)
                # Overflows after 2^64 blocks.  Not worth carrying.
                t = ctr[0] + 1; ctr[0] = t & 0xffffffff; ctr[1] += t
                buf[start + 64*i : start + 64*(i + 1)] = \
                    struct.pack('<IIIIIIIIIIIIIIII', *out)
            nextra = nbytes - 64*nblocks
            if 0 < nextra:
                chacha.core(8, out, ctr, subkey, const32)
                buf[start + 64*nblocks : start + 64*nblocks + nextra] = \
                    struct.pack('<IIIIIIIIIIIIIIII', *out)[0 : nextra]
            subkey[0:8] = out[0:8] = out[8:16] = self.zero

    def weakrandom_bytes(self, n):
        buf = bytearray(n)
        self.weakrandom_bytearray(buf, 0, n)
        return bytes(buf)

def weakprng(seed):
    return WeakPRNG(seed)

if weakprng(bytes(bytearray([0] * 32))).weakrandom64() != 0x42fe0c0eb8fd7b38:
    raise Exception('weakprng self-test failed')

nondeterministic_weakprng_local = threading.local()
nondeterministic_weakprng_local.prng = None

def nondeterministic_weakprng():
    if nondeterministic_weakprng_local.prng == None:
        nondeterministic_weakprng_local.prng = weakprng(os.urandom(32))
    return nondeterministic_weakprng_local.prng

def nondeterministic_weakrandom32():
    return nondeterministic_weakprng().weakrandom32()
def nondeterministic_weakrandom64():
    return nondeterministic_weakprng().weakrandom64()
def nondeterministic_weakrandom_uniform(n):
    return nondeterministic_weakprng().weakrandom_uniform(n)
def nondeterministic_weakrandom_bytearray(buf, start, end):
    return nondeterministic_weakprng().weakrandom_bytearray(buf, start, end)
def nondeterministic_weakrandom_bytes(n):
    return nondeterministic_weakprng().weakrandom_bytes(n)

nondeterministic_weakrandom32()
nondeterministic_weakrandom64()
nondeterministic_weakrandom_uniform(123)
nondeterministic_weakrandom_bytearray(bytearray([0] * 16), 3, 8)
nondeterministic_weakrandom_bytearray(bytearray([0] * 80), 3, 67)
nondeterministic_weakrandom_bytearray(bytearray([0] * 160), 3, 132)
nondeterministic_weakrandom_bytearray(bytearray([0] * 1024), 3, 1023)
nondeterministic_weakrandom_bytearray(bytearray([0] * 1024), 3, 963)
nondeterministic_weakrandom_bytes(42)
