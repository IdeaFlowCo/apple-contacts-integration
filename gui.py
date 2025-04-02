import sys
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, 
    QTableWidget, QTableWidgetItem, QLabel, QPushButton,
    QHBoxLayout, QHeaderView, QTextEdit, QDialog, QLineEdit,
    QFormLayout, QDialogButtonBox
)
from PyQt6.QtCore import Qt, QTimer
from Contacts import (
    CNContactStore, CNEntityType, CNContactFetchRequest, CNContactFormatter,
    CNEntityTypeContacts, CNContactFormatterStyleFullName, CNContactFormatter,
    CNContactSortOrderGivenName, CNContactGivenNameKey, CNContactFamilyNameKey,
    CNContactMiddleNameKey, CNContactNamePrefixKey, CNContactNameSuffixKey,
    CNContactNicknameKey, CNContactOrganizationNameKey, CNContactPhoneNumbersKey,
    CNContactEmailAddressesKey, CNContactNoteKey, CNContact, CNMutableContact,
    CNPhoneNumber, CNLabeledValue, CNSaveRequest, CNContactIdentifierKey,
    CNContactTypeKey
)
from Foundation import (
    NSLog, NSAutoreleasePool, NSObject, NSNotificationCenter, 
    NSRunLoop, NSDate, NSURL, NSNotificationName
)
from AppKit import NSWorkspace
from AddressBook import ABAddressBook
import objc
from libdispatch import dispatch_semaphore_create, dispatch_semaphore_wait, dispatch_semaphore_signal, DISPATCH_TIME_FOREVER
import time

# Reuse the key fetching code
def get_required_keys():
    """Get all required keys for contact fetching."""
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
        CNContactIdentifierKey,  # Use proper identifier key
        CNContactTypeKey,
    ]
    # Remove duplicates while preserving order
    seen = set()
    return [x for x in keys if not (x in seen or seen.add(x))]

# Required keys to fetch (including all necessary name components)
KEYS_TO_FETCH = get_required_keys()
CNContactStoreDidChangeNotification = NSNotificationName("CNContactStoreDidChangeNotification")

class ContactEditDialog(QDialog):
    def __init__(self, contact=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Edit Contact")
        self.setModal(True)
        
        layout = QFormLayout(self)
        
        # Create input fields
        self.first_name = QLineEdit(self)
        self.last_name = QLineEdit(self)
        self.organization = QLineEdit(self)
        self.phone = QLineEdit(self)
        self.email = QLineEdit(self)
        self.note = QTextEdit(self)
        self.note.setMaximumHeight(100)
        
        # Add fields to layout
        layout.addRow("First Name:", self.first_name)
        layout.addRow("Last Name:", self.last_name)
        layout.addRow("Organization:", self.organization)
        layout.addRow("Phone:", self.phone)
        layout.addRow("Email:", self.email)
        layout.addRow("Note:", self.note)
        
        # Add OK and Cancel buttons
        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addRow(buttons)
        
        # If editing existing contact, populate fields
        if contact:
            try:
                self.first_name.setText(contact.valueForKey_(CNContactGivenNameKey) or "")
                self.last_name.setText(contact.valueForKey_(CNContactFamilyNameKey) or "")
                self.organization.setText(contact.valueForKey_(CNContactOrganizationNameKey) or "")
                
                phones = contact.valueForKey_(CNContactPhoneNumbersKey)
                if phones and len(phones):
                    self.phone.setText(phones[0].value().stringValue())
                    
                emails = contact.valueForKey_(CNContactEmailAddressesKey)
                if emails and len(emails):
                    self.email.setText(emails[0].value())
                 
                # === Safely populate Note field ===
                if contact.isKeyAvailable_(CNContactNoteKey):
                    self.note.setText(contact.valueForKey_(CNContactNoteKey) or "")
                else:
                    self.note.setText("") # Set to empty if key wasn't available
                    print(f"Warning: CNContactNoteKey was not available when populating dialog for contact ID: {contact.identifier()}")
                # === End safe population ===

            except Exception as e:
                print(f"Error populating contact fields: {e}")
                # Log the error but don't clear the fields - let's see what we managed to populate
                print("Current field values:")
                print(f"First Name: {self.first_name.text()}")
                print(f"Last Name: {self.last_name.text()}")
                print(f"Organization: {self.organization.text()}")
                print(f"Phone: {self.phone.text()}")
                print(f"Email: {self.email.text()}")
                print(f"Note: {self.note.toPlainText()}")

        # === REMOVE focus setting from here ===
        # self.first_name.setFocus()
        # === End focus setting ===

class ContactsWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Contacts Monitor")
        self.setGeometry(100, 100, 800, 600)
        
        # Create main widget and layout
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout(main_widget)
        
        # Status bar for messages
        self.status_label = QLabel("Starting up...")
        self.status_label.setStyleSheet("QLabel { padding: 5px; }")
        layout.addWidget(self.status_label)
        
        # Create table for contacts
        self.contacts_table = QTableWidget()
        self.contacts_table.setColumnCount(5)  # Added column for actions
        self.contacts_table.setHorizontalHeaderLabels(["Name", "Phone", "Email", "Organization", "Actions"])
        header = self.contacts_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.Fixed)
        self.contacts_table.setColumnWidth(4, 100)  # Fixed width for actions column
        self.contacts_table.itemDoubleClicked.connect(self.handle_item_double_click)
        layout.addWidget(self.contacts_table)
        
        # Create log area
        self.log_area = QTextEdit()
        self.log_area.setReadOnly(True)
        self.log_area.setMaximumHeight(150)
        self.log_area.setStyleSheet("QTextEdit { font-family: monospace; }")
        layout.addWidget(self.log_area)
        
        # Button row
        button_layout = QHBoxLayout()
        
        # Add New Contact button
        self.new_contact_button = QPushButton("➕ New Contact")
        self.new_contact_button.clicked.connect(self.create_new_contact)
        button_layout.addWidget(self.new_contact_button)
        
        # Refresh button
        self.refresh_button = QPushButton("🔄 Refresh Contacts")
        self.refresh_button.clicked.connect(self.refresh_contacts)
        button_layout.addWidget(self.refresh_button)
        
        # Status indicator
        self.monitoring_label = QLabel("👀 Monitoring for changes...")
        self.monitoring_label.setStyleSheet("QLabel { color: green; }")
        button_layout.addWidget(self.monitoring_label)
        
        layout.addLayout(button_layout)
        
        # Initialize contacts monitoring
        self.store = None
        self.notification_center = None
        self.handler = None
        self.last_contact_count = 0
        self.initialize_contacts()
        
        # Setup periodic refresh timer
        self.timer = QTimer()
        self.timer.timeout.connect(self.process_events)
        self.timer.start(100)  # Check more frequently (every 100ms)
        
    def log(self, message, is_change=False):
        timestamp = time.strftime("%H:%M:%S")
        if is_change:
            message = f"\n{'='*50}\n🔄 {message}\n{'='*50}"
            self.status_label.setText(f"Last change: {timestamp}")
            self.status_label.setStyleSheet("QLabel { background-color: #e6ffe6; padding: 5px; }")
            # Reset the background after 2 seconds
            QTimer.singleShot(2000, lambda: self.status_label.setStyleSheet("QLabel { padding: 5px; }"))
        self.log_area.append(f"[{timestamp}] {message}")
        # Scroll to the bottom
        self.log_area.verticalScrollBar().setValue(self.log_area.verticalScrollBar().maximum())
        
    def initialize_contacts(self):
        self.store = CNContactStore.alloc().init()
        if not self.request_access():
            self.log("❌ Failed to get contacts access")
            return
            
        self.notification_center = NSNotificationCenter.defaultCenter()
        self.handler = ContactChangeHandler.alloc().init()
        self.handler.window = self
        self.handler.setStore_(self.store)
        
        # Register for notifications with more specific parameters
        self.notification_center.addObserver_selector_name_object_(
            self.handler,
            b'contactsChanged:',
            CNContactStoreDidChangeNotification,
            None  # Set to None to receive notifications from any contact store
        )
        
        self.log("🎯 Registered for contact change notifications")
        self.refresh_contacts()
        
    def request_access(self):
        semaphore = dispatch_semaphore_create(0)
        granted = [False]
        
        def completion_handler(success, error):
            granted[0] = success
            if error:
                self.log(f"Error requesting access: {error}")
            dispatch_semaphore_signal(semaphore)
        
        self.store.requestAccessForEntityType_completionHandler_(
            CNEntityTypeContacts,
            completion_handler
        )
        
        dispatch_semaphore_wait(semaphore, DISPATCH_TIME_FOREVER)
        return granted[0]
        
    def refresh_contacts(self):
        self.log("Refreshing contacts...")
        contacts = self.fetch_contacts()
        if contacts:
            prev_count = self.last_contact_count
            self.display_contacts(contacts)
            self.last_contact_count = len(contacts)
            
            if prev_count > 0 and prev_count != self.last_contact_count:
                diff = self.last_contact_count - prev_count
                if diff > 0:
                    self.log(f"Contact change detected: {diff} contact(s) added", True)
                else:
                    self.log(f"Contact change detected: {abs(diff)} contact(s) removed", True)
            else:
                self.log(f"✅ Loaded {len(contacts)} contacts")
        else:
            self.log("❌ Failed to fetch contacts")
            
    def fetch_contacts(self):
        pool = NSAutoreleasePool.alloc().init()
        all_contacts = []
        fetch_count = 0
        note_key_missing_count = 0
        
        try:
            request = CNContactFetchRequest.alloc().initWithKeysToFetch_(KEYS_TO_FETCH)
            request.setSortOrder_(CNContactSortOrderGivenName)
            
            def handle_contact(contact, stop):
                nonlocal fetch_count, note_key_missing_count
                fetch_count += 1
                # === Add logging for Note key availability ===
                if not contact.isKeyAvailable_(CNContactNoteKey):
                    note_key_missing_count += 1
                    # Log only if missing, to avoid spamming logs
                    self.log(f"Warning: CNContactNoteKey not available for contact ID: {contact.identifier()} during fetch.")
                # === End logging ===
                all_contacts.append(contact)
            
            self.log("Starting contact enumeration (unification disabled)...")
            success, error = self.store.enumerateContactsWithFetchRequest_error_usingBlock_(
                request, None, handle_contact
            )
            self.log(f"Finished contact enumeration. Fetched: {fetch_count}")
            if note_key_missing_count > 0:
                self.log(f"Warning: CNContactNoteKey was missing for {note_key_missing_count} out of {fetch_count} contacts during fetch.")
            
            if not success:
                self.log(f"Error fetching contacts: {error}")
                return None
                
            return all_contacts
        except Exception as e:
            self.log(f"Exception while fetching contacts: {e}")
            return None
        finally:
            del pool
            
    def display_contacts(self, contacts):
        self.contacts = contacts  # Store contacts for reference
        self.contacts_table.setRowCount(len(contacts))
        for i, contact in enumerate(contacts):
            try:
                # Name
                name = CNContactFormatter.stringFromContact_style_(contact, CNContactFormatterStyleFullName)
                if not name:
                    parts = []
                    given_name = contact.valueForKey_(CNContactGivenNameKey)
                    family_name = contact.valueForKey_(CNContactFamilyNameKey)
                    if given_name:
                        parts.append(given_name)
                    if family_name:
                        parts.append(family_name)
                    name = " ".join(parts) or "[No Name]"
                self.contacts_table.setItem(i, 0, QTableWidgetItem(name))
                
                # Phone
                phones = contact.valueForKey_(CNContactPhoneNumbersKey)
                if phones:
                    phone_str = ", ".join(p.value().stringValue() for p in phones)
                    self.contacts_table.setItem(i, 1, QTableWidgetItem(phone_str))
                
                # Email
                emails = contact.valueForKey_(CNContactEmailAddressesKey)
                if emails:
                    email_str = ", ".join(e.value() for e in emails)
                    self.contacts_table.setItem(i, 2, QTableWidgetItem(email_str))
                
                # Organization
                org = contact.valueForKey_(CNContactOrganizationNameKey)
                if org:
                    self.contacts_table.setItem(i, 3, QTableWidgetItem(org))
                    
                # Add edit button
                edit_button = QPushButton("✏️ Edit")
                edit_button.clicked.connect(lambda checked, row=i: self.edit_contact(row))
                self.contacts_table.setCellWidget(i, 4, edit_button)
                    
            except Exception as e:
                self.log(f"Error displaying contact {i}: {str(e)}")
                
    def handle_item_double_click(self, item):
        row = item.row()
        self.edit_contact(row)
        
    def edit_contact(self, row):
        contact = self.contacts[row]
        dialog = ContactEditDialog(contact, self)
        
        # === Set focus using QTimer after dialog is created ===
        QTimer.singleShot(0, lambda: dialog.first_name.setFocus())
        # === End focus setting ===
        
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self.save_contact_changes(contact, dialog)
            
    def save_contact_changes(self, contact, dialog):
        try:
            self.log(f"--- Starting save_contact_changes for contact ID: {contact.identifier()} ---")
            
            # Log available keys on the original contact
            available_keys = [key for key in KEYS_TO_FETCH if contact.isKeyAvailable_(key)]
            self.log(f"Keys available on original contact: {available_keys}")
            if CNContactIdentifierKey not in available_keys:
                self.log("CRITICAL: Original contact missing Identifier Key!")
                # Optionally, try fetching the contact again here if needed

            # Use mutableCopy again, now that we fetch all keys
            self.log("Attempting mutableCopy...")
            mutable_contact = contact.mutableCopy()
            self.log("mutableCopy() successful.")

            # Log the identifier we are about to update
            identifier = mutable_contact.identifier()
            self.log(f"Attempting to update contact with identifier: {identifier}")
            if not identifier:
                 self.log("CRITICAL: mutable_contact has no identifier after copy!")

            # Update the contact properties with new values from the dialog
            self.log(f"Setting GivenName: {dialog.first_name.text()}")
            mutable_contact.setValue_forKey_(dialog.first_name.text(), CNContactGivenNameKey)
            self.log(f"Setting FamilyName: {dialog.last_name.text()}")
            mutable_contact.setValue_forKey_(dialog.last_name.text(), CNContactFamilyNameKey)
            self.log(f"Setting OrganizationName: {dialog.organization.text()}")
            mutable_contact.setValue_forKey_(dialog.organization.text(), CNContactOrganizationNameKey)
            
            # === Try reading Note key before setting ===
            # try:
            #     self.log("Attempting to read Note key from mutable_contact before setting...")
            #     _ = mutable_contact.valueForKey_(CNContactNoteKey) # Try reading it
            #     self.log("Reading Note key successful (or key was nil).")
            # except Exception as read_err:
            #     # This might happen if the key truly wasn't available on the copy
            #     self.log(f"Warning: Could not read Note key from mutable_contact: {read_err}")
            # === End reading attempt ===
                
            # === Temporarily disable setting Note ===
            # self.log(f"Setting Note: {dialog.note.toPlainText()[:30]}...") # Log first 30 chars
            # mutable_contact.setValue_forKey_(dialog.note.toPlainText(), CNContactNoteKey)
            self.log("Skipping Note update due to fetching issues.")
            # === End Note disabling ===
            
            # Handle phone numbers
            self.log(f"Setting PhoneNumbers: {dialog.phone.text()}")
            if dialog.phone.text():
                phone_number = CNPhoneNumber.phoneNumberWithStringValue_(dialog.phone.text())
                phone_value = CNLabeledValue.labeledValueWithLabel_value_("main", phone_number)
                mutable_contact.setValue_forKey_([phone_value], CNContactPhoneNumbersKey)
            else:
                self.log("Clearing PhoneNumbers")
                mutable_contact.setValue_forKey_([], CNContactPhoneNumbersKey)
            
            # Handle email addresses
            self.log(f"Setting EmailAddresses: {dialog.email.text()}")
            if dialog.email.text():
                email_value = CNLabeledValue.labeledValueWithLabel_value_("main", dialog.email.text())
                mutable_contact.setValue_forKey_([email_value], CNContactEmailAddressesKey)
            else:
                self.log("Clearing EmailAddresses")
                mutable_contact.setValue_forKey_([], CNContactEmailAddressesKey)
            
            self.log("Finished setting properties on mutable_contact.")

            # Save the changes
            save_request = CNSaveRequest.alloc().init()
            self.log("Adding updateContact to save_request...")
            save_request.updateContact_(mutable_contact)
            self.log("updateContact added to save_request.")
            
            self.log(f"Executing save request for contact: {identifier}...")
            success, error = self.store.executeSaveRequest_error_(save_request, None)
            self.log(f"executeSaveRequest completed. Success: {success}")
            
            if success:
                self.log(f"✅ Contact {identifier} updated successfully", True)
                self.refresh_contacts()  # Refresh to show changes
            else:
                error_str = str(error) if error else "Unknown error"
                self.log(f"❌ Failed to update contact {identifier}: {error_str}", True)
                # Log details of the mutable contact that failed to save
                try:
                    failed_contact_details = {}
                    for key in KEYS_TO_FETCH:
                        if mutable_contact.isKeyAvailable_(key):
                             failed_contact_details[key] = mutable_contact.valueForKey_(key)
                        else:
                             failed_contact_details[key] = "<Not Available>"
                    self.log(f"Failed mutable_contact state: {failed_contact_details}")
                except Exception as detail_err:
                    self.log(f"Could not log details of failed contact: {detail_err}")

        except Exception as e:
            # Catch the specific exception if possible
            exception_type = type(e).__name__
            self.log(f"❌ Exception during contact update ({exception_type}): {str(e)}", True)
            # Add traceback maybe?
            import traceback
            self.log(traceback.format_exc()) # Log full traceback
        finally:
            self.log(f"--- Finished save_contact_changes for contact ID: {contact.identifier()} ---")

    def create_new_contact(self):
        dialog = ContactEditDialog(parent=self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            try:
                # Create a new mutable contact
                new_contact = CNMutableContact.alloc().init()
                
                # Set the contact properties
                new_contact.setValue_forKey_(dialog.first_name.text(), CNContactGivenNameKey)
                new_contact.setValue_forKey_(dialog.last_name.text(), CNContactFamilyNameKey)
                new_contact.setValue_forKey_(dialog.organization.text(), CNContactOrganizationNameKey)
                new_contact.setValue_forKey_(dialog.note.toPlainText(), CNContactNoteKey)
                
                # Handle phone numbers
                if dialog.phone.text():
                    phone_number = CNPhoneNumber.phoneNumberWithStringValue_(dialog.phone.text())
                    phone_value = CNLabeledValue.labeledValueWithLabel_value_("main", phone_number)
                    new_contact.setValue_forKey_([phone_value], CNContactPhoneNumbersKey)
                
                # Handle email addresses
                if dialog.email.text():
                    email_value = CNLabeledValue.labeledValueWithLabel_value_("main", dialog.email.text())
                    new_contact.setValue_forKey_([email_value], CNContactEmailAddressesKey)
                
                # Save the new contact
                save_request = CNSaveRequest.alloc().init()
                save_request.addContact_(new_contact)
                
                success, error = self.store.executeSaveRequest_error_(save_request, None)
                
                if success:
                    self.log("✅ New contact created successfully", True)
                    self.refresh_contacts()
                else:
                    self.log(f"❌ Failed to create contact: {error}", True)
                    
            except Exception as e:
                self.log(f"❌ Error creating contact: {str(e)}", True)

    def process_events(self):
        # Process any pending events in the run loop
        pool = NSAutoreleasePool.alloc().init()
        run_loop = NSRunLoop.currentRunLoop()
        next_date = NSDate.dateWithTimeIntervalSinceNow_(0.1)
        run_loop.runMode_beforeDate_("kCFRunLoopDefaultMode", next_date)
        del pool

class ContactChangeHandler(NSObject):
    def init(self):
        self = objc.super(ContactChangeHandler, self).init()
        if self is not None:
            self.store = None
            self.window = None
            self.last_change_time = time.time()
        return self

    def contacts_changed_(self, notification):
        change_time = time.time()
        pool = NSAutoreleasePool.alloc().init()
        
        try:
            detection_latency = change_time - self.last_change_time
            self.window.log(f"🔄 Contact change notification received! (Latency: {detection_latency:.2f}s)", True)
            self.window.refresh_contacts()
        except Exception as e:
            self.window.log(f"Error handling change notification: {e}")
        finally:
            self.last_change_time = change_time
            del pool

    def setStore_(self, store):
        self.store = store

    # Define the method signature for the notification handler
    contacts_changed_ = objc.selector(
        contacts_changed_,
        selector=b'contactsChanged:',
        signature=b'v@:@',
        isRequired=False
    )

def main():
    app = QApplication(sys.argv)
    window = ContactsWindow()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main() 