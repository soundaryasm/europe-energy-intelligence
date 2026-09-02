"""Synthetic ENTSO-E XML fixtures for tests (Spec 002).

These are hand-built from ENTSO-E's publicly documented schema shape.
They have NOT been captured from a real ENTSO-E response (no API
credentials were available). They exist to pin down this codebase's
parsing behaviour against a documented, versioned assumption — not to
assert that assumption is correct. Validate against a real payload before
trusting this pipeline in production.
"""

LOAD_XML = """<?xml version="1.0" encoding="UTF-8"?>
<GL_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0">
  <mRID>doc-load-123</mRID>
  <TimeSeries>
    <quantity_Measure_Unit.name>MAW</quantity_Measure_Unit.name>
    <Period>
      <timeInterval>
        <start>2024-01-01T00:00Z</start>
        <end>2024-01-01T02:00Z</end>
      </timeInterval>
      <resolution>PT60M</resolution>
      <Point><position>1</position><quantity>3500.5</quantity></Point>
      <Point><position>2</position><quantity>3400.2</quantity></Point>
    </Period>
  </TimeSeries>
</GL_MarketDocument>
"""

GENERATION_XML = """<?xml version="1.0" encoding="UTF-8"?>
<GL_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0">
  <mRID>doc-gen-456</mRID>
  <TimeSeries>
    <MktPSRType>
      <psrType>B19</psrType>
    </MktPSRType>
    <quantity_Measure_Unit.name>MAW</quantity_Measure_Unit.name>
    <Period>
      <timeInterval><start>2024-01-01T00:00Z</start></timeInterval>
      <resolution>PT60M</resolution>
      <Point><position>1</position><quantity>120.0</quantity></Point>
    </Period>
  </TimeSeries>
  <TimeSeries>
    <MktPSRType>
      <psrType>B16</psrType>
    </MktPSRType>
    <quantity_Measure_Unit.name>MAW</quantity_Measure_Unit.name>
    <Period>
      <timeInterval><start>2024-01-01T00:00Z</start></timeInterval>
      <resolution>PT60M</resolution>
      <Point><position>1</position><quantity>50.0</quantity></Point>
    </Period>
  </TimeSeries>
</GL_MarketDocument>
"""

PRICE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0">
  <mRID>doc-price-789</mRID>
  <TimeSeries>
    <currency_Unit.name>EUR</currency_Unit.name>
    <Period>
      <timeInterval><start>2024-01-01T00:00Z</start></timeInterval>
      <resolution>PT60M</resolution>
      <Point><position>1</position><price.amount>-5.32</price.amount></Point>
      <Point><position>2</position><price.amount>45.10</price.amount></Point>
    </Period>
  </TimeSeries>
</Publication_MarketDocument>
"""

ACKNOWLEDGEMENT_ERROR_XML = """<?xml version="1.0" encoding="UTF-8"?>
<Acknowledgement_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-1:acknowledgementdocument:6:1">
  <mRID>ack-1</mRID>
  <Reason>
    <code>999</code>
    <text>No matching data found for the requested period.</text>
  </Reason>
</Acknowledgement_MarketDocument>
"""
