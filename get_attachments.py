# Get your files that Gmail block. Warning message:
# "Anti-virus warning - 1 attachment contains a virus or blocked file. Downloading this attachment is disabled."
# Based on: http://spapas.github.io/2014/10/23/retrieve-gmail-blocked-attachments/
# Go to your emails, click the arrow button in the top right, "Show original", save to the same directory as this script.
 
import email
import sys
import os
 
if __name__ == '__main__':
  if len(sys.argv) < 2:
    print("Press enter to process all files with .txt extension.")
    input()
    files = [ f for f in os.listdir('.') if os.path.isfile(f) and f.endswith('.txt') ]
  else:
    files = sys.argv[1:]
 
  print("Files: %s" % ', '.join(files))
  print()
 
  for f in files:
    msg = email.message_from_file(open(f))
    print("Processing %s" % f)
    print("Subject: %s" % msg['Subject'])
    for pl in msg.get_payload():
      fn = pl.get_filename()
      if fn:
        print("Found %s" % fn)
        if os.path.isfile(fn):
          print("The file %s already exists! Press enter to overwrite." % fn)
          input()
        open(fn, 'wb').write(pl.get_payload(decode=True))
    print()
