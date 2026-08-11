# Calendar moving-average missingness contract

Production and challenger moving-average transforms share one governed rule.
Every source-origin-comparable series is expanded to an explicit month-end
calendar. A trailing *X*-month mean is available with at least
`ceil(X * 2/3)` valid observations and averages only those valid observations.
Missing observations remain null: they are not zero-filled, interpolated, or
forward-filled. Consequently, a missing evaluation month can contain a valid
computed feature, while retaining its identity as a calendar evaluation date
rather than a source observation.

MA-derived momentum references the exact calendar month at the configured lag.
A genuine source-origin change is a hard boundary. A gap bracketed by the same
governed origin remains comparable; a gap between different or ambiguous
origins is not blended. If coverage fails, the feature remains unavailable and
the existing downstream alignment and staleness behavior applies.

The motivating production case was the absent October 2025 BLS LAUS release
during the federal shutdown. The rule is generic and contains no LAUS- or
date-specific branch. The governed thresholds are MA3=2, MA6=4, MA9=6, and
MA12=8 valid observations.
