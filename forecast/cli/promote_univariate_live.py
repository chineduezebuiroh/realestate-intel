from __future__ import annotations
# forecast/cli/promote_univariate_live.py

import argparse

from forecast.core.db_forecast import new_batch_id
from forecast.models.sarimax_univariate.core import SarimaxUnivariateSpec
from forecast.runners.live_univariate import run_live_univariate


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("forecast.cli.promote_univariate_live")
    p.add_argument("--metric_id", required=True)
    p.add_argument("--geo_id", required=True)
    p.add_argument("--property_type_id", required=True)
    p.add_argument("--freq", default="M")
    p.add_argument("--horizon", type=int, required=True)
    p.add_argument("--data_asof", required=True, help="YYYY-MM-DD")
    p.add_argument("--artifact_root", default="artifacts/phasec")
    p.add_argument("--batch_id", default=None)
    p.add_argument("--model_version", default="phasec_live_v01")

    p.add_argument("--order", default="1,1,1")
    p.add_argument("--seasonal_order", default="1,1,1,12")
    p.add_argument("--enforce_stationarity", action="store_true")
    p.add_argument("--enforce_invertibility", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    batch_id = args.batch_id or f"phasec__live__{new_batch_id()}"

    order = tuple(int(x) for x in args.order.split(","))
    seas = tuple(int(x) for x in args.seasonal_order.split(","))

    spec = SarimaxUnivariateSpec(
        order=order,
        seasonal_order=seas,
        enforce_stationarity=bool(args.enforce_stationarity),
        enforce_invertibility=bool(args.enforce_invertibility),
    )

    audit = run_live_univariate(
        metric_id=args.metric_id,
        geo_id=args.geo_id,
        property_type_id=str(args.property_type_id),
        freq=args.freq,
        horizon=int(args.horizon),
        data_asof=args.data_asof,
        batch_id=batch_id,
        artifact_root=args.artifact_root,
        model_version=args.model_version,
        spec=spec,
    )

    print(f"[promote_univariate_live] run_id={audit['run_id']} batch_id={audit['batch_id']}")
    print(f"[promote_univariate_live] predictions={audit['artifacts']['predictions']}")
    print(f"[promote_univariate_live] audit={audit['artifacts']['audit']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
