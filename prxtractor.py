import re
import os
import sys
import argparse
from hashlib import md5
from typing import List, Optional, Tuple
from Crypto.Cipher import AES
from Crypto.Hash import CMAC


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DEFAULT_PHRASES_FILE = os.path.join(SCRIPT_DIR, "phrases.txt")

REPLAYREADY_TK_HEX = "8B222FFD1E76195659CF2703898C427F"
REPLAYREADY_IK_HEX = "9CE93432C7D74016BA684763F801E136"


def load_phrases(path: str) -> List[str]:
    if not os.path.isfile(path):
        return []
    phrases: List[str] = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.split("#", 1)[0].strip()
            if line:
                phrases.append(line)
    return phrases


def base_phrases_from(phrases: List[str]) -> List[str]:
    if not phrases:
        return []
    bases = set()
    for a in phrases:
        for b in phrases:
            if a != b and b.startswith(a):
                bases.add(a)
                break
    if not bases:
        bases.add(min(phrases, key=len))
    return sorted(bases)


def strings_dump(filepath: str, min_len: int = 6) -> Tuple[bytes, List[str]]:
    with open(filepath, "rb") as f:
        data = f.read()
    pattern = rb"[\x20-\x7E]{" + str(min_len).encode() + rb",}"
    return data, [m.group().decode("ascii") for m in re.finditer(pattern, data)]


def find_playready_phrases(filepath: str, known_phrases: List[str]) -> Optional[str]:
    data, hits = strings_dump(filepath, min_len=6)

    known_hints = ["pszBasePhrase", "pszAdditionalPhrase", "pszPhrase", "Salted__"]
    print("Known marker strings found:")
    found_markers = False
    for h in hits:
        if any(hint.lower() in h.lower() for hint in known_hints):
            print(f"  {h}")
            found_markers = True
    if not found_markers:
        print("  None.")

    print("\nPotential base + additional passphrase pairs:")
    passphrase = None
    concat_pattern = rb"([\x20-\x7E]{6,24})\x00+([\x20-\x7E]{6,24})"
    for m in re.finditer(concat_pattern, data):
        a, b = m.group(1).decode(), m.group(2).decode()
        if re.match(r"^[A-Za-z0-9]{6,24}$", a) and re.match(r"^[A-Za-z0-9]{6,24}$", b):
            if not passphrase:
                passphrase = a + b
            print(f"  '{a}' + '{b}' => '{a + b}'")
    if not passphrase:
        print("  None found.")

    if not passphrase and known_phrases:
        for base in base_phrases_from(known_phrases):
            if base.encode() in data:
                print(f"\nFound a known base phrase '{base}' in the binary.")
                print("The additional phrase may be model-specific. Try the candidates")
                print("below with --phrase, or supply your own.")
                for p in known_phrases:
                    print(f"  {p}")
                break

    return passphrase


def openssl_kdf(passphrase: str, salt: bytes) -> Tuple[bytes, bytes]:
    data = passphrase.encode()
    d, d_i = b"", b""
    while len(d) < 48:
        d_i = md5(d_i + data + salt).digest()
        d += d_i
    return d[:32], d[32:48]


def decrypt_salted(dat_path: str, passphrase: str, strip_padding: bool = False) -> Optional[str]:
    with open(dat_path, "rb") as f:
        raw = f.read()

    if raw[:8] != b"Salted__":
        print(f"  {dat_path} does not have an OpenSSL Salted__ header, skipping.")
        return None

    salt = raw[8:16]
    ciphertext = raw[16:]

    key, iv = openssl_kdf(passphrase, salt)
    decrypted = AES.new(key, AES.MODE_CBC, iv).decrypt(ciphertext)

    if strip_padding:
        decrypted = decrypted[:-8]
        print("  Stripped last 8 bytes (zgpriv padding).")

    pad_len = decrypted[-1]
    if 1 <= pad_len <= 16:
        decrypted = decrypted[:-pad_len]

    out_path = dat_path.replace(".dat", "_decrypted.bin")
    with open(out_path, "wb") as f:
        f.write(decrypted)
    print(f"  Decrypted to {out_path} ({len(decrypted)} bytes)")
    return out_path


def aes_key_unwrap(kek: bytes, wrapped: bytes) -> bytes:
    if len(wrapped) < 24 or len(wrapped) % 8 != 0:
        raise ValueError("wrapped data must be at least 24 bytes and a multiple of 8")
    cipher = AES.new(kek, AES.MODE_ECB)
    n = len(wrapped) // 8 - 1
    A = wrapped[:8]
    R = [wrapped[8 + i * 8 : 8 + (i + 1) * 8] for i in range(n)]
    for j in range(5, -1, -1):
        for i in range(n, 0, -1):
            t = (n * j) + i
            A_xor = bytes(a ^ b for a, b in zip(A, t.to_bytes(8, "big")))
            block = cipher.decrypt(A_xor + R[i - 1])
            A = block[:8]
            R[i - 1] = block[8:]
    if A != b"\xA6" * 8:
        raise ValueError("AIV mismatch (wrong KEK or corrupted input)")
    return b"".join(R)


def derive_replayready_kek() -> bytes:
    cmac_data = (
        b"\x01"
        + bytes.fromhex(REPLAYREADY_IK_HEX)
        + b"\x00"
        + b"\x00" * 16
        + b"\x00\x80"
    )
    cmac = CMAC.new(bytes.fromhex(REPLAYREADY_TK_HEX), ciphermod=AES)
    cmac.update(cmac_data)
    return cmac.digest()


def decrypt_zgpriv_protected(path: str) -> Optional[str]:
    with open(path, "rb") as f:
        wrapped = f.read()

    if len(wrapped) < 24:
        print(f"  {path} is too short to be a wrapped key ({len(wrapped)} bytes).")
        return None

    try:
        kek = derive_replayready_kek()
        unwrapped = aes_key_unwrap(kek, wrapped)
    except Exception as e:
        print(f"  AES key unwrap failed: {e}")
        print("  This file may use a different KEK than the PlayReady Porting Kit default.")
        return None

    zgpriv = unwrapped[:32]
    base, _ = os.path.splitext(path)
    out_path = base.replace("_protected", "") + "_decrypted.bin"
    with open(out_path, "wb") as f:
        f.write(zgpriv)
    print(f"  Unwrapped zgpriv saved to {out_path} ({len(zgpriv)} bytes)")
    return out_path


def looks_like_salted(path: str) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(8) == b"Salted__"
    except OSError:
        return False


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="prxtractor",
        description=(
            "Extract PlayReady passphrases from libplayready.so and decrypt the "
            "associated bgroupcert/zgpriv files. Also handles zgpriv_protected.dat "
            "wrapped with the PlayReady Porting Kit default Transient/Intermediate keys."
        ),
    )
    parser.add_argument(
        "so_path",
        nargs="?",
        help="Path to libplayready.so.0 (or any binary). "
             "Optional if you only want to unwrap a zgpriv_protected.dat.",
    )
    parser.add_argument(
        "dat_files",
        nargs="*",
        help="One or more .dat files to decrypt. Detected automatically: "
             "files starting with 'Salted__' use the passphrase path, others "
             "treated as PR-PK-wrapped zgpriv_protected.dat.",
    )
    parser.add_argument(
        "--phrase",
        default=None,
        help="Override the auto-detected passphrase. Used when the binary is "
             "stripped or uses a non-standard layout. See --list-phrases for "
             "known candidates.",
    )
    parser.add_argument(
        "--phrases-file",
        default=DEFAULT_PHRASES_FILE,
        help=f"Path to a text file of candidate passphrases, one per line. "
             f"Defaults to phrases.txt next to the script.",
    )
    parser.add_argument(
        "--list-phrases",
        action="store_true",
        help="Print the candidate passphrases from --phrases-file and exit.",
    )
    args = parser.parse_args()

    known_phrases = load_phrases(args.phrases_file)

    if args.list_phrases:
        if not known_phrases:
            print(f"No phrases loaded (file not found: {args.phrases_file}).")
            return 1
        print(f"Candidate passphrases from {args.phrases_file}:")
        for p in known_phrases:
            print(f"  {p}")
        return 0

    if not args.so_path and not args.dat_files:
        parser.print_help()
        return 2

    passphrase: Optional[str] = args.phrase
    if args.so_path:
        if not os.path.isfile(args.so_path):
            print(f"File not found: {args.so_path}")
            return 2
        print(f"Scanning {args.so_path}...\n")
        detected = find_playready_phrases(args.so_path, known_phrases)
        if not passphrase:
            passphrase = detected
        elif detected and detected != passphrase:
            print(f"\nOverriding detected passphrase '{detected}' with '{passphrase}'.")

    if passphrase:
        print(f"\nUsing passphrase: {passphrase}")

    for dat in args.dat_files:
        if not os.path.isfile(dat):
            print(f"\nFile not found: {dat}")
            continue
        print(f"\nProcessing {dat}...")
        base = os.path.basename(dat).lower()
        if looks_like_salted(dat):
            if not passphrase:
                print("  No passphrase available, cannot decrypt Salted__ file.")
                print("  Supply one with --phrase, or pass libplayready.so.0 first.")
                continue
            strip = "zgpriv" in base
            decrypt_salted(dat, passphrase, strip_padding=strip)
        else:
            decrypt_zgpriv_protected(dat)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())