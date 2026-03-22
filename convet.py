import json

input_file = "emails.txt"
output_file = "emails.json"

data = []

with open(input_file, "r") as f:
    for idx, line in enumerate(f, start=1):
        email = line.strip()
        
        if email:  # skip empty lines
            data.append({
                "id": idx,
                "email": email
            })

with open(output_file, "w") as f:
    json.dump(data, f, indent=2)

print(f"Saved {len(data)} emails to {output_file}")