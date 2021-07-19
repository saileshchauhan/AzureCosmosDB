import uuid

def get_sam_data():
    sam_item={
        "id": "1_"+str(uuid.uuid4()),
        "class": "first",
        "name": "sam"
    }
    return sam_item

def get_sandra_data():
    sandra_item={
        "id": "2_"+str(uuid.uuid4()),
        "class": "third",
        "name": "sandra"
    }
    return sandra_item
