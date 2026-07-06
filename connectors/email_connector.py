"""
email_connector.py — Reads CSV/Excel attachments from email inbox.
Used when institutions send data files via email.
Security: email credentials stored in .env file only.
"""

import imaplib
import email
import pandas as pd
from io import BytesIO
from utils.logger import log_event


class EmailConnector:
    """
    Connects to an email inbox via IMAP and downloads
    CSV/Excel attachments from unread emails.
    """

    def __init__(self, host, username, password, log_dir,
                 port=993, mailbox="INBOX"):
        self.host = host
        self.username = username
        self.password = password
        self.log_dir = log_dir
        self.port = port
        self.mailbox = mailbox

    def connect(self):
        """Connects to IMAP server and selects mailbox."""
        try:
            mail = imaplib.IMAP4_SSL(self.host, self.port)
            mail.login(self.username, self.password)
            mail.select(self.mailbox)
            log_event(self.log_dir, "INFO", "email_connector",
                      "ALL", f"Connected to email: {self.host}")
            return mail
        except Exception as e:
            log_event(self.log_dir, "ERROR", "email_connector",
                      "ALL", f"Email connection failed: {str(e)}")
            return None

    def get_unread_emails(self, mail):
        """Returns list of unread email IDs."""
        _, messages = mail.search(None, "UNSEEN")
        return messages[0].split()

    def extract_attachments(self, mail, email_id):
        """Extracts CSV/Excel attachments from a single email."""
        attachments = []
        try:
            _, msg_data = mail.fetch(email_id, "(RFC822)")
            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)

            for part in msg.walk():
                filename = part.get_filename()
                if filename and any(
                    filename.endswith(ext)
                    for ext in [".csv", ".xlsx", ".xls"]
                ):
                    data = part.get_payload(decode=True)
                    buffer = BytesIO(data)

                    if filename.endswith(".csv"):
                        df = pd.read_csv(buffer)
                    else:
                        df = pd.read_excel(buffer)

                    attachments.append({
                        "dataframe": df,
                        "source_file": filename,
                        "source_type": "email"
                    })
                    log_event(self.log_dir, "INFO", "email_connector",
                              "ALL", f"Extracted {filename}: {len(df)} rows")

        except Exception as e:
            log_event(self.log_dir, "ERROR", "email_connector",
                      "ALL", f"Failed to extract attachment: {str(e)}")

        return attachments

    def read_all(self):
        """Reads all unread emails and extracts attachments."""
        mail = self.connect()
        if not mail:
            return []

        results = []
        for email_id in self.get_unread_emails(mail):
            results.extend(self.extract_attachments(mail, email_id))

        mail.logout()
        log_event(self.log_dir, "INFO", "email_connector",
                  "ALL", f"Total attachments extracted: {len(results)}")
        return results
