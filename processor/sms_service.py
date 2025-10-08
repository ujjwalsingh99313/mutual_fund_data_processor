import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional
from config.config import (
    NIMBUS_UID,
    NIMBUS_PWD,
    NIMBUS_SENDERID,
    NIMBUS_ENTITYID,
    NIMBUS_TEMPLATEID,
    NIMBUS_URL,
    SIP_ALERT_TEST_MODE,
    SIP_ALERT_TEST_NUMBER,
)

logger = logging.getLogger(__name__)

class SMSService:
    def __init__(
        self,
        uid: str = NIMBUS_UID,
        pwd: str = NIMBUS_PWD,
        senderid: str = NIMBUS_SENDERID,
        entityid: str = NIMBUS_ENTITYID,
        templateid: str = NIMBUS_TEMPLATEID,
        base_url: str = NIMBUS_URL,
        timeout: int = 30,
        max_retries: int = 3,
        test_mode: bool = SIP_ALERT_TEST_MODE
    ):
        self.uid = uid
        self.pwd = pwd
        self.senderid = senderid
        self.entityid = entityid
        self.templateid = templateid
        self.base_url = base_url
        self.timeout = timeout
        self.test_mode = test_mode

        self.session = requests.Session()
        retries = Retry(
            total=max_retries,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"]),
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    @staticmethod
    def _normalize_mobile(mobile: Optional[str]) -> Optional[str]:
        if mobile is None:
            return None
        digits = "".join(filter(str.isdigit, str(mobile)))
        if len(digits) == 10:
            return f"91{digits}"
        if len(digits) == 12 and digits.startswith("91"):
            return digits
        if len(digits) == 13 and digits.startswith("0091"):
            return digits[-12:]
        return None

    def send_sms(self, mobile: str, message: str, extra_params: dict = None) -> bool:
        if self.test_mode:
            logger.info("Test mode active: sending all SMS to %s", SIP_ALERT_TEST_NUMBER)
            mobile = SIP_ALERT_TEST_NUMBER

        try:
            normalized = self._normalize_mobile(mobile)
            if not normalized:
                logger.error("Invalid mobile number provided: %s", mobile)
                return False

            if not message:
                logger.error("Empty message, aborting SMS send.")
                return False

            if len(message) > 1000:
                logger.warning("Message too long; truncating to 1000 chars.")
                message = message[:1000]

            params = {
                "uid": self.uid,
                "pwd": self.pwd,
                "sender": self.senderid,
                "entityid": self.entityid,
                "templateid": self.templateid,
                "mobile": normalized,
                "msg": message,
            }

            if extra_params:
                params.update(extra_params)

            logger.debug("Sending SMS to %s; params keys: %s", normalized, list(params.keys()))
            response = self.session.get(self.base_url, params=params, timeout=self.timeout)

            if response.status_code == 200:
                logger.info("SMS sent successfully to %s", normalized)
                return True
            else:
                logger.error("SMS sending failed. status=%s, text=%s", response.status_code, response.text)
                return False

        except Exception as exc:
            logger.exception("Exception while sending SMS to %s: %s", mobile, exc)
            return False

    def send_sip_alert(self, mobile: str, investor_name: str, scheme_name: str, sip_amount: float, sip_date: str) -> bool:
        try:
            amount_text = f"₹{sip_amount:,}" if isinstance(sip_amount, (int, float)) else str(sip_amount)
            message = (
                f"Dear {investor_name}, your SIP of {amount_text} for {scheme_name} will be processed on {sip_date}. "
                f"Ensure sufficient funds in your bank account. - Vedant Asset"
            )
            return self.send_sms(mobile=mobile, message=message)
        except Exception as e:
            logger.exception("Failed to build/send SIP alert for %s: %s", mobile, e)
            return False

# Single instance
sms_service = SMSService()
