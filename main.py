import sys
from Contacts import (
    CNContactStore, CNEntityType, CNContactFetchRequest, CNContactFormatter,
    CNEntityTypeContacts, CNContactFormatterStyleFullName, CNContactFormatter,
    CNContactSortOrderGivenName, CNContactGivenNameKey, CNContactFamilyNameKey,
    CNContactMiddleNameKey, CNContactNamePrefixKey, CNContactNameSuffixKey,
    CNContactNicknameKey, CNContactOrganizationNameKey, CNContactPhoneNumbersKey,
    CNContactEmailAddressesKey, CNContactNoteKey, CNContactIdentifierKey,
    CNContactTypeKey
)
from Foundation import (
    NSLog, NSAutoreleasePool, NSObject, NSNotificationCenter, 
    NSRunLoop, NSDate, NSURL, NSArray
)
from AppKit import NSWorkspace
from AddressBook import ABAddressBook
import objc
from libdispatch import dispatch_semaphore_create, dispatch_semaphore_wait, dispatch_semaphore_signal, DISPATCH_TIME_FOREVER
import time

def get_required_keys():
    """Get all required keys for contact fetching."""
    # Create an array of all the keys we want
    keys = [
        CNContactGivenNameKey,
        CNContactFamilyNameKey,
        CNContactMiddleNameKey,
        CNContactNamePrefixKey,
        CNContactNameSuffixKey,
        CNContactNicknameKey,
        CNContactOrganizationNameKey,
        CNContactPhoneNumbersKey,
        CNContactEmailAddressesKey,
        CNContactNoteKey,
        CNContactIdentifierKey,
        CNContactTypeKey,
    ]
    
    # Remove duplicates while preserving order
    seen = set()
    return [x for x in keys if not (x in seen or seen.add(x))]

# Required keys to fetch (including all necessary name components)
KEYS_TO_FETCH = get_required_keys()

# Notification constant
CNContactStoreDidChangeNotification = "CNContactStoreDidChangeNotification"

def log(message):
    """Log a message with timestamp."""
    print(f"[{time.strftime('%H:%M:%S')}] {message}")

def ensure_contacts_permissions():
    """Try to ensure contacts permissions are granted."""
    try:
        # Try multiple approaches to trigger the permission dialog
        workspace = NSWorkspace.sharedWorkspace()
        
        # Try different URL schemes for different macOS versions
        urls_to_try = [
            "x-apple.systempreferences:com.apple.settings.PrivacySecurity.extension?Privacy_Contacts",  # newer macOS
            "x-apple.systempreferences:com.apple.preference.security?Privacy_Contacts",  # older macOS
            "file:///System/Applications/Contacts.app"  # fallback to Contacts.app
        ]
        
        opened = False
        for url in urls_to_try:
            if workspace.openURL_(NSURL.URLWithString_(url)):
                opened = True
                break
        
        if not opened:
            log("Could not open System Settings automatically")
        
        # Also try using AddressBook which can trigger the dialog
        try:
            ab = ABAddressBook.sharedAddressBook()
            if ab:
                ab.people()
        except:
            pass
            
    except Exception as e:
        log(f"Note: Could not pre-check permissions: {e}")
        
    print("\n⚠️  Permission Required")
    print("=" * 50)
    print("This app needs access to your contacts to work.")
    print("\nTo grant access:")
    print("1. Open System Settings")
    print("2. Go to Privacy & Security > Contacts")
    print("3. Find Terminal or Python in the list")
    print("4. Toggle the switch to enable access")
    print("5. Run this script again")
    print("\nIf you don't see Terminal/Python in the list:")
    print("• Try running: sudo tccutil reset Contacts")
    print("• Then quit Terminal completely (Cmd+Q)")
    print("• Reopen Terminal and run this script again")
    print("=" * 50 + "\n")

def request_access(store):
    """Request access to contacts."""
    ensure_contacts_permissions()
    
    # Create a semaphore for synchronization
    semaphore = dispatch_semaphore_create(0)
    granted = [False]  # Use list to allow modification in callback
    
    def completion_handler(success, error):
        granted[0] = success
        if error:
            log(f"Error requesting access: {error}")
        dispatch_semaphore_signal(semaphore)
    
    # Get current authorization status
    auth_status = CNContactStore.authorizationStatusForEntityType_(CNEntityTypeContacts)
    log(f"Initial authorization status: {auth_status}")
    
    # Request access
    store.requestAccessForEntityType_completionHandler_(
        CNEntityTypeContacts,
        completion_handler
    )
    
    # Wait for the completion handler
    dispatch_semaphore_wait(semaphore, DISPATCH_TIME_FOREVER)
    
    if not granted[0]:
        print("\n❌ Contact access denied!")
        print("Please grant access to contacts:")
        print("1. Open System Settings")
        print("2. Go to Privacy & Security > Contacts")
        print("3. Enable access for Terminal/Python")
        print("4. Run this script again\n")
        return False
    
    return True

def fetch_contacts(store):
    """Fetches all contacts from the store."""
    pool = NSAutoreleasePool.alloc().init()
    log("Fetching contacts...")
    all_contacts = []
    
    try:
        # Create a fetch request with all required keys
        request = CNContactFetchRequest.alloc().initWithKeysToFetch_(KEYS_TO_FETCH)
        request.setSortOrder_(CNContactSortOrderGivenName)
        
        def handle_contact(contact, stop):
            # Simply append the contact directly without creating a new one
            all_contacts.append(contact)
        
        success, error = store.enumerateContactsWithFetchRequest_error_usingBlock_(
            request, None, handle_contact
        )

        if not success:
            log(f"Error fetching contacts: {error}")
            del pool
            return None

        log(f"Fetched {len(all_contacts)} contacts successfully")
        return all_contacts
    except Exception as e:
        log(f"Exception while fetching contacts: {e}")
        return None
    finally:
        del pool

def display_contacts(contacts):
    """Displays the fetched contacts."""
    if not contacts:
        print("No contacts to display.")
        return

    print("\n📱 Contact List:")
    print("="*70)
    
    for i, contact in enumerate(contacts, 1):
        try:
            # Get the display name
            display_name = CNContactFormatter.stringFromContact_style_(contact, CNContactFormatterStyleFullName)
            if not display_name:
                parts = []
                try:
                    given_name = contact.valueForKey_(CNContactGivenNameKey)
                    if given_name:
                        parts.append(given_name)
                except:
                    pass
                try:
                    family_name = contact.valueForKey_(CNContactFamilyNameKey)
                    if family_name:
                        parts.append(family_name)
                except:
                    pass
                display_name = " ".join(parts) or "[No Name]"
            
            # Add organization name if available
            try:
                org_name = contact.valueForKey_(CNContactOrganizationNameKey)
                if org_name:
                    display_name += f" ({org_name})"
            except:
                pass
                
            print(f"\n{i}. {display_name}")
            
            # Show phone numbers
            try:
                phones = contact.valueForKey_(CNContactPhoneNumbersKey)
                if phones:
                    for phone in phones:
                        print(f"   📱 {phone.value().stringValue()}")
            except:
                pass
            
            # Show email addresses
            try:
                emails = contact.valueForKey_(CNContactEmailAddressesKey)
                if emails:
                    for email in emails:
                        print(f"   ✉️ {email.value()}")
            except:
                pass
            
            # Show notes if available
            try:
                note = contact.valueForKey_(CNContactNoteKey)
                if note:
                    note_lines = note.split('\n')
                    print(f"   📝 Notes:")
                    for line in note_lines:
                        print(f"      {line}")
            except:
                pass
        except Exception as e:
            log(f"Error displaying contact {i}: {str(e)}")
    print("\n" + "="*70)

# Define a class to handle notifications
class ContactChangeHandler(NSObject):
    @objc.signature(b'v@:@')
    def contacts_changed_(self, notification):
        # Record the time when we receive the notification
        change_time = time.time()
        
        # Create an autorelease pool for this thread
        pool = NSAutoreleasePool.alloc().init()
        
        print("\n" + "="*70)
        print("🔄 CONTACT CHANGE DETECTED!")
        print("="*70)
        
        if hasattr(self, 'store') and self.store:
            log("Fetching updated contacts...")
            fetch_start = time.time()
            updated_contacts = fetch_contacts(self.store)
            fetch_end = time.time()
            
            if updated_contacts is not None:
                # Calculate and display timing metrics
                detection_latency = change_time - self.last_change_time
                fetch_duration = fetch_end - fetch_start
                
                print("\n⏱ TIMING METRICS:")
                print(f"   • Change Detection Latency: {detection_latency:.3f} seconds")
                print(f"   • Fetch & Display Time: {fetch_duration:.3f} seconds")
                print(f"   • Total Update Time: {(fetch_duration + detection_latency):.3f} seconds\n")
                
                display_contacts(updated_contacts)
            else:
                log("Failed to fetch contacts after change notification.")
        else:
            log("Contact store not available in handler.")
        
        # Update last change time
        self.last_change_time = change_time
        del pool

    def setStore_(self, store):
        self.store = store
        self.last_change_time = time.time()  # Initialize the last change time

def main():
    pool = NSAutoreleasePool.alloc().init()
    store = CNContactStore.alloc().init()

    if not request_access(store):
        print("Contact access is required to run this program.", file=sys.stderr)
        del pool
        sys.exit(1)

    contacts = fetch_contacts(store)
    if contacts is not None:
        display_contacts(contacts)
    else:
        print("Failed to fetch contacts.", file=sys.stderr)
        del pool
        sys.exit(1)

    notification_center = NSNotificationCenter.defaultCenter()
    handler = ContactChangeHandler.alloc().init()
    handler.setStore_(store)

    print("\n👀 Watching for contact changes...")
    print("   • Open the Contacts app and make a change to test")
    print("   • The script will detect changes in real-time")
    print("   • Press Ctrl+C to exit\n")
    
    notification_center.addObserver_selector_name_object_(
        handler,
        b'contacts_changed:',
        CNContactStoreDidChangeNotification,
        store
    )

    run_loop = NSRunLoop.currentRunLoop()
    try:
        # Run the loop with a date in the future, checking periodically
        while True:
            # Process events for 1 second at a time
            next_date = NSDate.dateWithTimeIntervalSinceNow_(1.0)
            if not run_loop.runMode_beforeDate_("kCFRunLoopDefaultMode", next_date):
                break
    except KeyboardInterrupt:
        print("\n\n👋 Stopping contact monitoring...")
    finally:
        notification_center.removeObserver_(handler)
        del handler
        del pool
        print("Goodbye!")

if __name__ == "__main__":
    main() 