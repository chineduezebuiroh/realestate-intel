import json

from sources.redfin.validate import validate_baseline


def main() -> int:
    print(json.dumps(validate_baseline(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
