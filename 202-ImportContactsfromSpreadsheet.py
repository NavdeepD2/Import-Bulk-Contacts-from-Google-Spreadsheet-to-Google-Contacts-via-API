import csv
import os.path
import pickle
from datetime import date
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# OAuth 2.0 Client ID scope for Google Contacts API
CONTACTS_SCOPES = ['https://www.googleapis.com/auth/contacts']

# Path to the CSV file
CSV_FILE_PATH = 'bulk_import_contacts_4mar.csv'

# Path to the token file
TOKEN_PICKLE_FILE = 'token.mcn.pickle'

# Dictionary to map city names to their shortened versions
CITY_SHORTENINGS = {
    'ADARSH NAGAR': 'ADRSH NGR',
    'ADIANA ROAD': 'ADIANA RD',
    'AJIT SINGH NAGAR': 'AJIT NGR',
    'AKALGARH': 'AKLGRH',
    'ANMOL CITY': 'ANMOL CT',
    'BAIRSAL KALAN': 'BAIRSAL',
    'BALIBEG COLONY': 'BALIBEG',
    'BALMIK MOHALLA': 'BALMIK MHL',
    'BANK COLONY': 'BANK CL',
    'BAZIGAR MOHALLA': 'BZGR MHL',
    'BEHRAMPUR': 'BPB',
    'BENIPAL COLONY': 'BENIPL CL',
    'BERI MOHALLA': 'BERI MHL',
    'BHORLA': 'BHORLA',
    'BURJ PAKKA': 'BURJ PK',
    'BUS STAND ROAD': 'BUS STD RD',
    'CHAKK LOHAT': 'CHAKK',
    'CHAKLI ADIL': 'CHK ADL',
    'CHARAN KANWAL CHOWK': 'CKC',
    'CHARAN KANWAL ROAD': 'CKR',
    'CHHAURIAN': 'CHRRN',
    'DALLA': 'DALLA',
    'DASHMESH NAGAR': 'DSHMS NGR',
    'DHAKA COLONY': 'DHAKA CL',
    'FATEHGARH VIRAN': 'FTHGRH VRN',
    'FRIENDS COLONY': 'FRNDS CL',
    'GAME SHAH': 'GAME SHAH',
    'GARHI': 'GARHI',
    'GARHI PULL': 'GARHI PL',
    'GARHI TARKHANA': 'GARHI TRKHN',
    'GAUNSGARH': 'GNSGRH',
    'GOBIND NAGAR': 'GOBND NGR',
    'GRAIN MARKET': 'GRAIN MKT',
    'GURON COLONY': 'GURON CL',
    'HAMBOWAL BET': 'HMB',
    'HAYATPUR': 'HYT',
    'HEDON ADDA': 'HEDON',
    'HEDON BET': 'HEDON',
    'INDIRA COLONY': 'INDIRA CL',
    'JASDEV NAGAR': 'JSDV NGR',
    'KACHA MACHHIWARA': 'KCMH',
    'KAHANPUR': 'KNPR',
    'KALGHIDHAR NAGAR': 'KLGHDR NGR',
    'KHALSA CHOWK': 'KHLS CHWK',
    'KHOKHAR': 'KHKHR',
    'KIRPAN BHET ROAD': 'KRPN RD',
    'KOHARA ROAD': 'KHR RD',
    'KOTALA': 'KTL',
    'KRISHNA PURI': 'KRSHN PURI',
    'LAKHOWAL': 'LKHWL',
    'LAXMI MARKET': 'LXMI MKT',
    'MACHHIWARA': 'MCH',
    'MACHHIWARA SAHIB': 'MCH',
    'MAHAVIR COLONY': 'MHVRV CL',
    'MAIN BAZAAR': 'MN BZR',
    'MAIN MACHHIWARA': 'MAIN MCH',
    'MAJRA': 'MJR',
    'MAKOWAL': 'MKWL',
    'MANGAT COLONY': 'MNGT CL',
    'MEHTOT': 'MHTT',
    'MIAN MOHALA': 'MIAN MHL',
    'MIAN MOHALLA': 'MIAN MHL',
    'MITHEWAL': 'MITHWL',
    'MOHAN MAJRA': 'MHN MJR',
    'NEW MAKOWAL': 'N MKWL',
    'NRI COLONY': 'NRI CL',
    'PANSARI CHOWK': 'PNSR CHWK',
    'PREM NAGAR': 'PRM NGR',
    'RAHON ROAD': 'RHN RD',
    'RAIA MOHALLA': 'RAIA MHL',
    'RAIPUR': 'RAIPR',
    'RAMGARH': 'RMGRH',
    'RAMPUR': 'RMPR',
    'RANJIT NAGAR': 'RNJT NGR',
    'RATTIPUR': 'RTTPR',
    'RATTIPUR ROAD': 'RTTPR RD',
    'RAVIDAS MOHALA': 'RVDS MHL',
    'ROPAR ROAD': 'RPR RD',
    'S.A.S NAGAR': 'SAS',
    'S.J.S NAGAR': 'SJS',
    'S.J.S NAGAR 2': 'SJS2',
    'SALAHPUR': 'SLHPR',
    'SALANA': 'SLN',
    'SAMRALA ROAD': 'SMRL RD',
    'SEHJO MAJRA': 'SHJ MJR',
    'SHERGARH': 'SHRGRH',
    'SHERPUR': 'SHRPR',
    'SHERPUR BASTI': 'SHRPR BSTI',
    'SIKANDARPUR BET': 'SKNDR PR',
    'SUNDAR MERKET': 'SNDR MKT',
    'SUNDER NAGAR': 'SNDR NGR',
    'TANDA KALIA': 'TND KL',
    'TAPPRIAN': 'TPPRN',
    'URNA BHARTHALA': 'URNA',
    # Add more city mappings here
}


def get_contacts_credentials():
    # Use OAuth 2.0 for managing Google Contacts
    creds = None
    # The file token.pickle stores the user's access and refresh tokens
    if os.path.exists(TOKEN_PICKLE_FILE):
        with open(TOKEN_PICKLE_FILE, 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'desktop.json', CONTACTS_SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(TOKEN_PICKLE_FILE, 'wb') as token:
            pickle.dump(creds, token)
    return creds


def read_data_from_csv():
    with open(CSV_FILE_PATH, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip the header row
        data = list(reader)
    return data


def create_contact(service, cust_id, cust_fname, cust_city, cust_phone):
    # Check if the phone number already exists
    contacts = service.people().connections().list(
        resourceName='people/me',
        pageSize=10,
        personFields='phoneNumbers'
    ).execute().get('connections', [])

    for contact in contacts:
        if 'phoneNumbers' in contact:
            for phoneNumber in contact['phoneNumbers']:
                if phoneNumber.get('value') == cust_phone:
                    print(f"Contact with the {cust_phone} phone number already exists. Skipping...")
                    return False

    # If the phone number doesn't exist, create a new contact
    contact_name = f"{cust_id} {cust_fname.split()[0]}"
    if cust_city in CITY_SHORTENINGS:
        contact_name += f" - {CITY_SHORTENINGS[cust_city]}"
    else:
        contact_name += f" - {cust_city.split()[0]}"

    # Create today's date label
    today = date.today().isoformat()
    label_id = f"today_{today.replace('-', '')}"
    create_label(service, label_id)

    contact = {
        "names": [
            {
                "givenName": contact_name
            }
        ],
        "phoneNumbers": [
            {
                "value": cust_phone,
                "type": "mobile"
            }
        ],
        "memberships": [
            {
                "contactGroupMembership": {
                    "contactGroupId": label_id
                }
            }
        ]
    }
    try:
        service.people().createContact(body=contact).execute()
        print(f"Contact created successfully. - {contact_name}-{cust_phone}")
        return True
    except HttpError as err:
        print(f"Error creating contact: {err}")
        return False


def create_label(service, label_id):
    try:
        service.contactGroups().get(
            resourceName=f"contactGroups/{label_id}"
        ).execute()
    except HttpError as err:
        if err.resp.status == 404:
            # Label doesn't exist, create it
            label_body = {
                "contactGroup": {
                    "name": "Today's Contacts",
                    "groupType": "USER_CONTACT_GROUP"
                }
            }
            service.contactGroups().create(body=label_body).execute()
            print("Label created successfully.")


def main():
    # Get OAuth 2.0 credentials for Google Contacts
    contacts_creds = get_contacts_credentials()
    contacts_service = build('people', 'v1', credentials=contacts_creds)

    # Read data from the CSV file
    data = read_data_from_csv()

    contacts_added = 0
    contacts_skipped = 0
    added_contacts_info = []
    skipped_contacts_info = []

    for row in data:
        cust_id = row[2]
        cust_fname = row[3]
        cust_city = row[7]
        cust_phone = row[5]

        if create_contact(contacts_service, cust_id, cust_fname, cust_city, cust_phone):
            contacts_added += 1
            added_contacts_info.append((cust_id, cust_fname, cust_city, cust_phone))
        else:
            contacts_skipped += 1
            skipped_contacts_info.append((cust_id, cust_fname, cust_city, cust_phone))

    print(f"Contacts added: {contacts_added}")
    print(f"Contacts skipped: {contacts_skipped}")
    print("Added contacts:")
    for contact_info in added_contacts_info:
        print(contact_info)
    print("Skipped contacts:")
    for contact_info in skipped_contacts_info:
        print(contact_info)


if __name__ == '__main__':
    main()
