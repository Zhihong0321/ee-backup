import json

def enrich_schema():
    with open("schema_metadata.json", "r") as f:
        schema = json.load(f)

    for table_name, table_data in schema.items():
        # Heuristics for Table Descriptions
        if not table_data["description"]:
            if "invoice" in table_name:
                table_data["description"] = "Stores financial invoice records and details."
            elif "customer" in table_name:
                table_data["description"] = "Contains customer profiles and contact information."
            elif "agent" in table_name:
                table_data["description"] = "Records related to sales agents or system agents."
            elif "seda" in table_name:
                table_data["description"] = "Data related to SEDA (Sustainable Energy Development Authority) registrations."
            elif "log" in table_name:
                table_data["description"] = "System or audit logs."

        for col in table_data["columns"]:
            # Skip if already described (unless it's the generic auto-gen one)
            current_desc = col["description"]
            
            new_desc = current_desc
            name = col["name"]
            dtype = col["type"]

            # Heuristics for Column Descriptions
            if name == "bubble_id":
                new_desc = "Unique identifier from the Bubble.io application."
            elif name == "synced_at" or name == "last_synced_at":
                new_desc = "Timestamp of the last synchronization with the external system."
            elif name == "created_by":
                new_desc = "ID or Name of the user/system that created this record."
            elif name == "modified_date" or name == "updated_at":
                new_desc = "Timestamp when the record was last modified."
            elif name == "created_date" or name == "created_at":
                new_desc = "Timestamp when the record was created."
            elif name.startswith("linked_"):
                target = name.replace("linked_", "").replace("_", " ").title()
                if dtype == "ARRAY":
                    new_desc = f"List of references to the {target} table."
                else:
                    new_desc = f"Reference to the {target} table."
            elif name == "email":
                new_desc = "Email address of the entity."
            elif name == "status" or name.endswith("_status"):
                new_desc = "Current status or state of the record."
            elif name == "active" or name == "is_active":
                new_desc = "Boolean flag indicating if the record is currently active."
            elif name == "file" or name.endswith("_link") or name.endswith("_pdf") or name.endswith("_image"):
                new_desc = "URL or path to the associated file/document."
            elif name == "total_cost" or name == "amount" or name == "price":
                new_desc = "Monetary value."
            elif name == "qty" or name == "quantity":
                new_desc = "Count or quantity of items."
            elif name == "user_input":
                new_desc = "Raw input provided by the user."
            elif name == "metadata":
                new_desc = "Additional JSON data for context."

            # Update if we found a better description or it was empty
            if new_desc and (not current_desc or current_desc.startswith("Timestamp")): 
                col["description"] = new_desc

    with open("schema_metadata.json", "w") as f:
        json.dump(schema, f, indent=4)
    print("Schema enriched.")

if __name__ == "__main__":
    enrich_schema()
