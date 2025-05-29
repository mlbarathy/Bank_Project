json_query = """
WITH parsed AS (
  SELECT
    get_json_object(value, '$.BkToCstmrStmt.GrpHdr.MsgId') AS MsgId,
    get_json_object(value, '$.BkToCstmrStmt.GrpHdr.Cretr.Nm') AS CreatorName,
    get_json_object(value, '$.BkToCstmrStmt.GrpHdr.Fr.Nm') AS FromName,
    from_json(
      get_json_object(value, '$.BkToCstmrStmt.Stmt.Ntry'),
      'array<struct<
        CdtDbtInd:string,
        Sts:string,
        NtryDtls:struct<
          TxDtls:struct<
            Refs:struct<EndToEndId:string>,
            IntrBkSttlmAmt:struct<Ccy:string,Value:string>
          >
        >
      >>'
    ) AS NtryArray
  FROM messageView
)
SELECT
  MsgId,
  CreatorName,
  FromName,
  ntry.CdtDbtInd,
  ntry.Sts,
  ntry.NtryDtls.TxDtls.Refs.EndToEndId AS EndToEndId,
  ntry.NtryDtls.TxDtls.IntrBkSttlmAmt.Ccy AS SettlementCurrency,
  ntry.NtryDtls.TxDtls.IntrBkSttlmAmt.Value AS SettlementValue
FROM parsed
LATERAL VIEW explode(NtryArray) AS ntry
"""


