import logging
import psycopg2
from datetime import datetime, timedelta
from typing import List, Dict

from config.config import DB_NAME, DB_USER, DB_PASS, DB_HOST, DB_PORT, SIP_ALERT_DAYS_BEFORE
from processor.sms_service import sms_service

logger = logging.getLogger(__name__)

class SIPAlertService:
    def __init__(self):
        self.db_config = {
            'dbname': DB_NAME,
            'user': DB_USER,
            'password': DB_PASS,
            'host': DB_HOST,
            'port': DB_PORT
        }

    def get_db_connection(self):
        """Create database connection"""
        return psycopg2.connect(**self.db_config)

    def get_investors_with_upcoming_sip(self, days_before: int = 1) -> List[Dict]:
        """Get investors whose SIP day equals the target day (today + days_before)."""
        try:
            conn = self.get_db_connection()
            with conn.cursor() as cursor:
                target_date = (datetime.now() + timedelta(days=days_before)).date()
                target_day = target_date.day

                query = """
                SELECT pan, inv_name, folio_no, scheme, isin, sip_amount, sip_date, mobile, email
                FROM public.fn_sip_calander(%s, %s, %s) sc
                WHERE sc.sip_date = %s
                AND sc.mobile IS NOT NULL
                """
                params = ('Admin', 1, target_date, target_day)
                cursor.execute(query, params)
                rows = cursor.fetchall()

            investors = []
            for row in rows:
                investors.append({
                    'pan': row[0],
                    'inv_name': row[1],
                    'folio_no': row[2],
                    'scheme': row[3],
                    'isin': row[4],
                    'sip_amount': float(row[5]) if row[5] is not None else 0,
                    'sip_date': int(row[6]) if row[6] is not None else None,
                    'mobile': row[7],
                    'email': row[8]
                })

            conn.close()
            logger.info("Found %d investors with SIP due on day %s", len(investors), target_day)
            return investors

        except Exception as e:
            logger.exception("Error fetching upcoming SIP investors: %s", e)
            return []

    def send_sip_alerts(self) -> Dict:
        """
        Send SIP alerts for configured SIP_ALERT_DAYS_BEFORE.
        Returns summary dict with success/failure lists and counts.
        """
        try:
            days_before = int(SIP_ALERT_DAYS_BEFORE) if SIP_ALERT_DAYS_BEFORE is not None else 1
            investors = self.get_investors_with_upcoming_sip(days_before)

            success = []
            failed = []

            for inv in investors:
                target_date = (datetime.now() + timedelta(days=days_before))
                sip_display_date = f"{inv.get('sip_date')}/{target_date.month}/{target_date.year}"

                mobile = inv.get('mobile')
                normalized = sms_service._normalize_mobile(mobile)
                if not normalized:
                    logger.error("Skipping invalid mobile for %s: %s", inv.get('inv_name'), mobile)
                    failed.append({'investor': inv, 'reason': 'invalid_mobile'})
                    continue

                sent = sms_service.send_sip_alert(
                    mobile=mobile,
                    investor_name=inv.get('inv_name') or 'Investor',
                    scheme_name=inv.get('scheme') or '',
                    sip_amount=inv.get('sip_amount') or 0,
                    sip_date=sip_display_date
                )

                if sent:
                    success.append({'investor': inv, 'mobile': normalized})
                    self.log_alert_sent(inv, 'SMS')
                else:
                    failed.append({'investor': inv, 'mobile': normalized})
                    self.log_alert_failed(inv, 'SMS')

            summary = {
                'success_count': len(success),
                'failure_count': len(failed),
                'total_investors': len(investors),
                'success_list': success,
                'failure_list': failed
            }

            logger.info("SIP alerts summary: %s", summary)
            return summary

        except Exception as e:
            logger.exception("Error in send_sip_alerts: %s", e)
            return {
                'success_count': 0,
                'failure_count': 0,
                'total_investors': 0,
                'error': str(e)
            }

    def log_alert_sent(self, investor, alert_type):
        logger.info("%s alert sent to %s (%s) for SIP amount ₹%s",
                    alert_type,
                    investor.get('inv_name'),
                    investor.get('mobile'),
                    investor.get('sip_amount'))

    def log_alert_failed(self, investor, alert_type):
        logger.error("%s alert failed for %s (%s)",
                     alert_type,
                     investor.get('inv_name'),
                     investor.get('mobile'))


# single instance
sip_alert_service = SIPAlertService()
