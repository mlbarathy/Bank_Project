from confluent_kafka import Producer
import random
import json
import time
from datetime import datetime, timedelta
import uuid

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'json_events'
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# Random Data Sources
BANK_NAMES = ["SIB", "DIB", "ENBD"]
CURRENCIES = ["GBP", "USD", "EUR"]
NAMES = ["Alice Smith", "Bob Johnson", "Charlie Lee", "Dana White", "Eliot Kim"]

def random_date(start_days_ago=10, end_days_ago=0):
    start_date = datetime.utcnow() - timedelta(days=start_days_ago)
    end_date = datetime.utcnow() - timedelta(days=end_days_ago)
    return start_date + (end_date - start_date) * random.random()

def generate_sample_json():
    fr_dt = random_date(30, 10).date().isoformat()
    to_dt = random_date(9, 1).date().isoformat()
    cre_dt_tm = datetime.utcnow().isoformat()

    msg_id = f"MSG{uuid.uuid4().hex[:10].upper()}"
    stmt_id = f"STATEMENT{uuid.uuid4().hex[:6].upper()}"
    org_id_from = str(random.randint(100000000, 999999999))
    org_id_to = str(random.randint(100000000, 999999999))
    iban = f"GB{random.randint(10,99)}NWBK601613{random.randint(10000000,99999999)}"
    currency = random.choice(CURRENCIES)
    fr_name = random.choice(NAMES)
    to_name = random.choice(NAMES)
    cretr_name = random.choice(BANK_NAMES)

    json_data = {
        "BkToCstmrStmt": {
            "GrpHdr": {
                "MsgId": msg_id,
                "CreDtTm": cre_dt_tm,
                "Authstn": {"Prtry": "BankAuthCode123"},
                "Cretr": {"Nm": cretr_name},
                "Fr": {"Nm": fr_name, "Id": {"OrgId": {"Othr": {"Id": org_id_from}}}},
                "To": {"Nm": to_name, "Id": {"OrgId": {"Othr": {"Id": org_id_to}}}},
            },
            "Stmt": {
                "Id": stmt_id,
                "ElctrncSeqNb": "1",
                "CreDtTm": cre_dt_tm,
                "FrToDt": {"FrDt": fr_dt, "ToDt": to_dt},
                "RptgSeqNb": "1",
                "Acct": {"Id": {"IBAN": iban}, "Ccy": currency},
                "Bal": [{
                    "Tp": {"Cd": "CLBD"},
                    "Amt": {"Ccy": currency, "Value": "1100.00"}
                }],
                "Ntry": [{
                    "Amt": {"Ccy": currency, "Value": "-150.00"},
                    "CdtDbtInd": "DBIT",
                    "Sts": "BOOK",
                    "NtryDtls": {
                        "TxDtls": {
                            "Refs": {"EndToEndId": "REF1234567890", "TxId": "TXID001"},
                            "TxTp": {"Cd": "SEPA"},
                            "RmtInf": {"Ustrd": "Payment for invoice 123"},
                            "IntrBkSttlmAmt": {"Ccy": currency, "Value": "150.00"}
                        }
                    }
                }, {
                    "Amt": {"Ccy": currency, "Value": "250.00"},
                    "CdtDbtInd": "CRDT",
                    "Sts": "BOOK",
                    "NtryDtls": {
                        "TxDtls": {
                            "Refs": {"EndToEndId": "REF0987654321", "TxId": "TXID002"},
                            "TxTp": {"Cd": "SEPA"},
                            "RmtInf": {"Ustrd": "Salary payment for December"},
                            "IntrBkSttlmAmt": {"Ccy": currency, "Value": "250.00"}
                        }
                    }
                }],
                "StmtPtnt": {
                    "Tp": {"Cd": "ATM"},
                    "Txt": f"Statement generated for the period {fr_dt} to {to_dt}"
                }
            }
        }
    }

    return json_data

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

def produce_messages():
    while True:
        message = generate_sample_json()
        producer.produce(
            TOPIC,
            key=message['BkToCstmrStmt']['GrpHdr']['MsgId'],
            value=json.dumps(message),
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(5)

if __name__ == "__main__":
    print("🔁 Producing Kafka messages every 5 seconds...")
    produce_messages()
