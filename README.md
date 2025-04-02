# Apple Contacts Sync

A Python script that syncs contacts from macOS Contacts app using `pyobjc-framework-Contacts`.

## Features

* Access and display contacts from macOS Contacts
* Real-time contact updates (planned)
* Simple command-line interface

## Requirements

* macOS
* Python 3
* `pyobjc-framework-Contacts`

## Installation

1. Clone this repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

```bash
python main.py
```

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