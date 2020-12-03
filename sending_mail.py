import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders


class MailSend:
    def __init__(self):
        self.distributor_name = None

    def invalid_api(self):
        email_user = "rpa@ahwspl.com"
        email_send = "rahul.pathak@ahwspl.com, pratik.chachad@ahwspl.com"
        cc = "rahul.pathak@ahwspl.com, rahulspath@gmail.com"

        rcpt = cc.split(",") + email_send.split(",") + [email_send]

        subject = (
            "RPA Python "
            + self.distributor_name
            + " Sales order RetailIO Production API services Invalid Response"
        )

        msg = MIMEMultipart()
        msg["From"] = email_user
        msg["To"] = email_send
        msg["subject"] = subject
        msg["Cc"] = cc

        body = (
            "Hi Team,\nRPA Python "
            + self.distributor_name
            + " has invalid API Response/Invalid API Key, so unable to punch the orders.\nKindly resolve the isssue!!\n\nRegards,\nRPA Team"
        )

        msg.attach(MIMEText(body, "plain"))
        # filename = 'SO Input.csv'
        # attachment = open(filename, 'rb')
        part = MIMEBase("application", "octet-stream")
        # part.set_payload((attachment).read())
        # encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment; filename= ")
        # , "attachment; filename= " + filename
        # msg.attach(part)
        text = msg.as_string()

        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(email_user, "Rpa@ahwspl2019Oct")

        server.sendmail(email_user, rcpt, text)
        server.quit()
        print("Mail Sent for Invalid API Response")

    def version_mismatch(self):
        email_user = "rpa@ahwspl.com"
        email_send = "pratik050793@gmail.com, pratik.chachad@ahwspl.com, rahulcoolrahul@yahoo.co.in"
        cc = "rahul.pathak@ahwspl.com, rahulspath@gmail.com"

        rcpt = cc.split(",") + email_send.split(",") + [email_send]

        subject = (
            "RPA Python "
            + self.distributor_name
            + "     ERP Version mismatch or ERP Server down"
        )

        msg = MIMEMultipart()
        msg["From"] = email_user
        msg["To"] = email_send
        msg["subject"] = subject
        msg["Cc"] = cc

        body = (
            "Hi Team ,\nUnable to Login to "
            + self.distributor_name
            + " ERP Application due to server issue or ERP Version mismatch, so unable to punch the orders.\nKindly check!!\n\nRegards,\nRPA Team."
        )

        msg.attach(MIMEText(body, "plain"))
        # filename = 'SO Input.csv'
        # attachment = open(filename, 'rb')
        part = MIMEBase("application", "octet-stream")
        # part.set_payload((attachment).read())
        # encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment; filename= ")
        # , "attachment; filename= " + filename
        # msg.attach(part)
        text = msg.as_string()

        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(email_user, "Rpa@ahwspl2019Oct")

        server.sendmail(email_user, rcpt, text)
        server.quit()
        print("Mail Sent for Version Mismatch/Server Down")

    def erp_db_error(self):
        email_user = "rpa@ahwspl.com"
        email_send = "rahul.pathak@ahwspl.com, pratik.chachad@ahwspl.com"
        cc = "rahul.pathak@ahwspl.com, rahulspath@gmail.com"

        rcpt = cc.split(",") + email_send.split(",") + [email_send]

        subject = (
            "RPA Python " + self.distributor_name + " ERP Database Connection Issue"
        )

        msg = MIMEMultipart()
        msg["From"] = email_user
        msg["To"] = email_send
        msg["subject"] = subject
        msg["Cc"] = cc

        body = (
            "Hi Team ,\n "
            + self.distributor_name
            + " ERP Database connectivity issue for Purchase order, so unable to punch the orders. Kindly check!!\n\nRegards,\nRPA Team"
        )

        msg.attach(MIMEText(body, "plain"))
        # filename = 'SO Input.csv'
        # attachment = open(filename, 'rb')
        part = MIMEBase("application", "octet-stream")
        # part.set_payload((attachment).read())
        # encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment; filename= ")
        # , "attachment; filename= " + filename
        # msg.attach(part)
        text = msg.as_string()

        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(email_user, "Rpa@ahwspl2019Oct")

        server.sendmail(email_user, rcpt, text)
        server.quit()
        print("Mail Sent for ERP DB Issue")

    def supplier_not_found(self):
        email_user = "rpa@ahwspl.com"
        email_send = "rahul.pathak@ahwspl.com, pratik.chachad@ahwspl.com"
        cc = "rahul.pathak@ahwspl.com, rahulspath@gmail.com"

        rcpt = cc.split(",") + email_send.split(",") + [email_send]

        subject = (
            "RPA Python "
            + self.distributor_name
            + " Purchase order Supplier code not found in ERP Application"
        )

        msg = MIMEMultipart()
        msg["From"] = email_user
        msg["To"] = email_send
        msg["subject"] = subject
        msg["Cc"] = cc

        body = (
            "Hi Team ,\nRpa Python "
            + self.distributor_name
            + " ERP Application Supplier Not Found, so unable to punch the orders. Kindly check!!\n\nRegards,\nRPA Team"
        )

        msg.attach(MIMEText(body, "plain"))
        # filename = 'SO Input.csv'
        # attachment = open(filename, 'rb')
        part = MIMEBase("application", "octet-stream")
        # part.set_payload((attachment).read())
        # encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment; filename= ")
        # , "attachment; filename= " + filename
        # msg.attach(part)
        text = msg.as_string()

        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(email_user, "Rpa@ahwspl2019Oct")

        server.sendmail(email_user, rcpt, text)
        server.quit()
        print("Mail Sent for Supplier Not Found")

    def po_null(self):
        email_user = "rpa@ahwspl.com"
        email_send = "rahul.pathak@ahwspl.com, pratik.chachad@ahwspl.com"
        cc = "rahul.pathak@ahwspl.com, rahulspath@gmail.com"

        rcpt = cc.split(",") + email_send.split(",") + [email_send]

        subject = (
            "RPA Python "
            + self.distributor_name
            + " ERP Purchase order record null value"
        )

        msg = MIMEMultipart()
        msg["From"] = email_user
        msg["To"] = email_send
        msg["subject"] = subject
        msg["Cc"] = cc

        body = (
            "Hi Team ,\nRPA Python "
            + self.distributor_name
            + " ERP Application Purchase order record has Null values, so unable to punch the orders. Kindly check!!\n\nRegards,\nRPA Team"
        )

        msg.attach(MIMEText(body, "plain"))
        # filename = 'SO Input.csv'
        # attachment = open(filename, 'rb')
        part = MIMEBase("application", "octet-stream")
        # part.set_payload((attachment).read())
        # encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment; filename= ")
        # , "attachment; filename= " + filename
        # msg.attach(part)
        text = msg.as_string()

        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(email_user, "Rpa@ahwspl2019Oct")

        server.sendmail(email_user, rcpt, text)
        server.quit()
        print("Mail Sent for PO Null Value")

    def sql_db_error(self):
        email_user = "rpa@ahwspl.com"
        email_send = "rahul.pathak@ahwspl.com, pratik.chachad@ahwspl.com"
        cc = "rahul.pathak@ahwspl.com, rahulspath@gmail.com"

        rcpt = cc.split(",") + email_send.split(",") + [email_send]

        subject = (
            "RPA Python "
            + self.distributor_name
            + " SQL Production Database not Working for Purchase Order"
        )

        msg = MIMEMultipart()
        msg["From"] = email_user
        msg["To"] = email_send
        msg["subject"] = subject
        msg["Cc"] = cc

        body = (
            "Hi Team ,\nRPA Python "
            + self.distributor_name
            + " SQL production database not working for Purchase Order. Kindly resolve the issue.\n\nRegards,\nRPA Team"
        )

        msg.attach(MIMEText(body, "plain"))
        # filename = 'SO Input.csv'
        # attachment = open(filename, 'rb')
        part = MIMEBase("application", "octet-stream")
        # part.set_payload((attachment).read())
        # encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment; filename= ")
        # , "attachment; filename= " + filename
        # msg.attach(part)
        text = msg.as_string()

        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(email_user, "Rpa@ahwspl2019Oct")

        server.sendmail(email_user, rcpt, text)
        server.quit()
        print("Mail Sent for SQL DB Error")


# m = MailSend()
# m.version_mismatch()