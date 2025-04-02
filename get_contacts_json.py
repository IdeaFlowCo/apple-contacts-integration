import sys
import json
import time
import objc
from Contacts import (
    CNContactStore, CNEntityType, CNContactFetchRequest,
    CNEntityTypeContacts, CNContactSortOrderGivenName, CNContactGivenNameKey,
    CNContactFamilyNameKey, CNContactMiddleNameKey, CNContactNamePrefixKey,
    CNContactNameSuffixKey, CNContactNicknameKey, CNContactOrganizationNameKey,
    CNContactPhoneNumbersKey, CNContactEmailAddressesKey, CNContactNoteKey,
    CNContactIdentifierKey, CNContactTypeKey
)
from Foundation import NSAutoreleasePool
from libdispatch import dispatch_semaphore_create, dispatch_semaphore_wait, dispatch_semaphore_signal, DISPATCH_TIME_FOREVER

def log_stderr(message):
    """Log a message to stderr."""
    print(f"[{time.strftime('%H:%M:%S')}][Python] {message}", file=sys.stderr)

def get_required_keys():
    """Get all required keys for contact fetching."""
    keys = [
        CNContactGivenNameKey,
        CNContactFamilyNameKey,
        # CNContactMiddleNameKey, # Usually not needed for basic sync
        # CNContactNamePrefixKey,
        # CNContactNameSuffixKey,
        # CNContactNicknameKey,
        CNContactOrganizationNameKey,
        CNContactPhoneNumbersKey,
        CNContactEmailAddressesKey,
        CNContactNoteKey,
        CNContactIdentifierKey, # Essential for tracking
        # CNContactTypeKey # Usually not needed for basic sync
    ]
    seen = set()
    return [x for x in keys if not (x in seen or seen.add(x))]

KEYS_TO_FETCH = get_required_keys()

def request_access(store):
    """Request access to contacts, exits if denied."""
    # Create a semaphore for synchronization
    semaphore = dispatch_semaphore_create(0)
    granted = [False]  # Use list to allow modification in callback

    def completion_handler(success, error):
        granted[0] = success
        if error:
            log_stderr(f"Error requesting access: {error}")
        dispatch_semaphore_signal(semaphore)

    # Get current authorization status
    auth_status = CNContactStore.authorizationStatusForEntityType_(CNEntityTypeContacts)
    log_stderr(f"Contact access status: {auth_status}") # 0: NotDetermined, 1: Restricted, 2: Denied, 3: Authorized

    if auth_status == 0: # Not Determined
        log_stderr("Requesting contact access...")
        store.requestAccessForEntityType_completionHandler_(
            CNEntityTypeContacts,
            completion_handler
        )
        # Wait for the user's response
        dispatch_semaphore_wait(semaphore, DISPATCH_TIME_FOREVER)
    elif auth_status == 3: # Authorized
        granted[0] = True
    else: # Restricted or Denied
        granted[0] = False

    if not granted[0]:
        log_stderr("Contact access denied or restricted.")
        log_stderr("Please grant access in System Settings > Privacy & Security > Contacts.")
        sys.exit(1) # Exit if access is not granted

    log_stderr("Contact access granted.")
    return True

def fetch_contacts(store):
    """Fetches all contacts from the store."""
    pool = NSAutoreleasePool.alloc().init()
    log_stderr("Fetching contacts...")
    all_contacts_raw = []

    try:
        request = CNContactFetchRequest.alloc().initWithKeysToFetch_(KEYS_TO_FETCH)
        request.setSortOrder_(CNContactSortOrderGivenName)

        # Use ObjC block directly for potentially better performance/stability
        success, error = store.enumerateContactsWithFetchRequest_error_usingBlock_(
            request,
            None,
            lambda contact, stop: all_contacts_raw.append(contact)
        )

        if not success:
            log_stderr(f"Error fetching contacts: {error}")
            return None

        log_stderr(f"Fetched {len(all_contacts_raw)} raw contacts successfully")
        return all_contacts_raw
    except Exception as e:
        log_stderr(f"Exception while fetching contacts: {e}")
        return None
    finally:
        del pool

def format_contacts_to_json(contacts_raw):
    """Formats CNContact objects into a JSON serializable list."""
    contacts_list = []
    for contact in contacts_raw:
        pool = NSAutoreleasePool.alloc().init() # Pool per contact iteration
        try:
            contact_dict = {
                "identifier": contact.identifier(),
                "givenName": contact.givenName() if contact.isKeyAvailable_(CNContactGivenNameKey) else None,
                "familyName": contact.familyName() if contact.isKeyAvailable_(CNContactFamilyNameKey) else None,
                "organizationName": contact.organizationName() if contact.isKeyAvailable_(CNContactOrganizationNameKey) else None,
                "note": contact.note() if contact.isKeyAvailable_(CNContactNoteKey) else None,
                "phoneNumbers": [],
                "emailAddresses": []
            }

            # Process Phone Numbers
            if contact.isKeyAvailable_(CNContactPhoneNumbersKey):
                phones = contact.phoneNumbers()
                if phones:
                    for labeled_value in phones:
                        phone_number = labeled_value.value().stringValue()
                        label = labeled_value.label() # Can be None
                        contact_dict["phoneNumbers"].append({"label": label, "value": phone_number})

            # Process Email Addresses
            if contact.isKeyAvailable_(CNContactEmailAddressesKey):
                emails = contact.emailAddresses()
                if emails:
                    for labeled_value in emails:
                        email_address = labeled_value.value()
                        label = labeled_value.label() # Can be None
                        contact_dict["emailAddresses"].append({"label": label, "value": email_address})

            contacts_list.append(contact_dict)
        except Exception as e:
            identifier = "Unknown"
            try:
                 identifier = contact.identifier()
            except: pass
            log_stderr(f"Error processing contact {identifier}: {e}")
        finally:
            del pool # Release pool for this contact

    return contacts_list

if __name__ == "__main__":
    store = CNContactStore.alloc().init()

    if request_access(store):
        raw_contacts = fetch_contacts(store)
        if raw_contacts is not None:
            formatted_contacts = format_contacts_to_json(raw_contacts)
            # Print the final JSON array to stdout
            print(json.dumps(formatted_contacts, indent=2))
            log_stderr(f"Successfully processed and output {len(formatted_contacts)} contacts as JSON.")
        else:
            log_stderr("Failed to fetch contacts.")
            sys.exit(1)
    else:
        # request_access already logged and exited if needed
        pass 