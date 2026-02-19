"""
Test Token Generator
Generates a file of fake FCM-style device tokens for load testing.

Usage:
    python generate_test_tokens.py --count 20000000 --output tokens.txt
"""

import argparse
import secrets
import sys


def generate_fake_fcm_token() -> str:
    """
    Real FCM tokens are ~152 chars, alphanumeric + hyphens + underscores + colons.
    We generate a plausible fake one for testing.
    """
    # Typical FCM token format: long base64-url-like string
    part1 = secrets.token_urlsafe(32)   # ~43 chars
    part2 = secrets.token_urlsafe(32)
    part3 = secrets.token_urlsafe(16)
    return f"{part1}:{part2}:{part3}"


def main():
    parser = argparse.ArgumentParser(description="Generate fake FCM tokens for testing")
    parser.add_argument("--count", type=int, default=1_000_000, help="Number of tokens to generate")
    parser.add_argument("--output", default="tokens.txt", help="Output file path")
    parser.add_argument("--batch-size", type=int, default=100_000, help="Write buffer size")
    args = parser.parse_args()

    print(f"Generating {args.count:,} test tokens → {args.output}")
    written = 0

    with open(args.output, "w", buffering=1 << 20) as f:  # 1MB write buffer
        batch = []
        for _ in range(args.count):
            batch.append(generate_fake_fcm_token())
            if len(batch) >= args.batch_size:
                f.write("\n".join(batch) + "\n")
                written += len(batch)
                batch.clear()
                print(f"\r  {written:>12,} / {args.count:,} tokens written", end="", flush=True)
        if batch:
            f.write("\n".join(batch) + "\n")
            written += len(batch)

    print(f"\nDone. {written:,} tokens written to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
