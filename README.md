# Apple Contacts Sync

This Python script demonstrates how to access and display contacts from the macOS Contacts app using the `pyobjc-framework-Contacts` library.

## Features

*   Requests user authorization to access contacts.
*   Fetches all contacts.
*   Displays a list of contact names.
*   (Planned) Listens for changes in the contacts database and updates the display automatically.

## Requirements

*   macOS
*   Python 3
*   `pyobjc-framework-Contacts`

## Installation

1.  Clone this repository (if applicable).
2.  Install the required library:
    ```bash
    pip install -r requirements.txt
    ```

## Important: macOS Contacts Permission

⚠️ **This application requires permission to access your Contacts!**

When you first run the application, you should see a permission dialog. However, if you don't see the dialog or if you previously denied access, follow these steps:

1. Open System Settings (previously System Preferences)
2. Click on "Privacy & Security"
3. Click on "Contacts" in the left sidebar
4. If you don't see Python or Terminal in the list:
   - Try running the script once using `pipenv run python main.py`
   - If still not appearing, try these steps:
     1. Close Terminal completely (Cmd+Q)
     2. Reopen Terminal
     3. Run the script again
     4. The permission dialog should now appear
5. Once you see Python/Terminal in the list, make sure to check the box next to it

If you're still having issues:
- Try removing and re-adding Terminal's permission if it's already in the list
- You might need to restart your computer in some cases
- Make sure you're running the script from Terminal, not another terminal emulator

## Usage

```bash
python main.py
```

The first time you run the script, macOS will prompt you for permission to access your contacts. You need to grant permission for the script to work.

## How it Works

The script uses the native macOS Contacts framework via PyObjC bindings:

1.  `CNContactStore`: Represents the user's contacts database.
2.  `requestAccessForEntityType:completionHandler:`: Prompts the user for permission.
3.  `authorizationStatusForEntityType:`: Checks the current permission status.
4.  `CNContactFetchRequest`: Defines which contact details (keys) to fetch.
5.  `enumerateContactsWithFetchRequest:error:usingBlock:`: Iterates through the contacts matching the request.
6.  `CNContactFormatter`: Used to get localized display names for contacts.

## Next Steps

*   Implement real-time change detection using `NSNotificationCenter` and `CNContactStoreDidChangeNotification`.
*   Potentially add a GUI interface instead of console output.
*   Explore 2-way synchronization (updating Apple Contacts from the app).

## Troubleshooting

If you're having permission issues:
1. Make sure you're running the latest version of macOS
2. Check if Terminal has Full Disk Access in Privacy & Security settings
3. Try running the script with `sudo` (though this isn't recommended for regular use)
4. Check Console.app for any permission-related error messages 