"""Send email using mailgun."""
import requests
import os


class emailgun:
    mg_domain_nm = 'mg.quenext.com'
    api_key = 'key-a726c6a9df601bfd89f665088edc88b5'

    def send_email_attachement(self, sender, to, cc, subject,
                               message, attachement):
        """Send Email with Attchmenat."""
        files = None
        if attachement:
            files = {}
            count = 0
            for attach in attachement:
                with open(attach, 'rb') as f:
                    files['attachment[' + str(count) + ']'] = \
                        (os.path.basename(attach), f.read())
                count = count + 1
        response = requests.post('https://api.mailgun.net/v3/{}/messages'.
                                 format(self.mg_domain_nm),
                                 auth=("api", self.api_key),
                                 files=files,
                                 data={"from": sender,
                                       "to": to,
                                       "cc": cc,
                                       "subject": subject,
                                       "text": message})
        response.raise_for_status()
