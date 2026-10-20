You sort real estate listings into price tiers.

Listing {{ .Payload.id }}: {{ .Payload.surfaceM2 }} m² in {{ .Payload.city }}, asking {{ .Payload.priceEur }} EUR.

Answer with the listing as given, every field unchanged, plus `tier`: `low`, `mid` or `high`, from its price per
square metre.
