import os
from forecast.models.sarimax_exog.canonicalize_v10 import CanonicalizeConfig, build_canonical_exogs_v10

cfg = CanonicalizeConfig(
    artifact_root=os.environ["ARTIFACT_ROOT"],
    input_stability_version=os.environ.get("CANON_IN", "v09.0"),
    output_stability_version=os.environ.get("CANON_OUT", "v10.0"),
    metric_id=os.environ["METRIC"],
    geo_id=os.environ["GEO"],
    property_type_id=os.environ["PT"],
    data_asof=os.environ["ASOF"],
    anchor_date=os.environ["ANCHOR"],
    horizon=int(os.environ.get("HORIZON", "12")),
    min_train_len=int(os.environ.get("MIN_TRAIN_LEN", "87")),
    max_exogs_out=int(os.environ.get("MAX_EXOGS_OUT", "30")),
)

out_csv, out_audit = build_canonical_exogs_v10(cfg)
print("WROTE", out_csv)
print("WROTE", out_audit)
