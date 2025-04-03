# Apple Contacts Integration

A TypeScript/Node.js application that integrates with macOS Contacts using Python for native contact access.

## Features

* Access and display contacts from macOS Contacts
* Export contacts as JSON
* TypeScript/Node.js interface with Python backend
* Real-time contact change detection and syncing

## Requirements

* macOS
* Node.js
* Python 3
* `pyobjc-framework-Contacts`
* Mew account and user root URL

## Installation

1. Clone this repository
2. Install Node.js dependencies:
   ```bash
   npm install
   ```
3. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Build TypeScript:
   ```bash
   npx tsc
   ```

## Usage

1. Build the TypeScript code:
   ```bash
   npx tsc
   ```
2. Run the application with your Mew user root URL:
   ```bash
   node dist/mewContacts.js <your_mew_user_root_url>
   ```
   Replace `<your_mew_user_root_url>` with your Mew user root URL.

The application will:
- Start listening for contact changes
- Perform an initial sync of all contacts
- Automatically sync any changes to contacts in real-time
- Restart the sync process if it crashes

## Permission Setup

⚠️ **Contacts Access Required**

1. Open System Settings → Privacy & Security → Contacts
2. If Python/Terminal isn't listed:
   - Run the script once
   - If still not appearing, restart Terminal and try again
3. Enable permission for Python/Terminal

## Troubleshooting

If you encounter permission issues:
1. Ensure Terminal has Full Disk Access in Privacy & Security
2. Try removing and re-adding Terminal's permission
3. Restart your computer if issues persist
4. Check Console.app for error messages 