import os

from pymongo import MongoClient
from gridfs import GridFS
from bson import ObjectId

MONGO_ADDRESS = 'mongodb://localhost:27017/'
MONGO_DB_NAME = 'test'
GRID_FS_COLLECTION_NAME = 'gridfs_collection'
INPUT_FILE_PATH = '/Users/mark/Downloads/Telegram Desktop/geek-notes-logo.png'
OUTPUT_FILE_PATH = '/Users/mark/Downloads/downloaded.png'


def upload(fs: GridFS, file_path: str) -> ObjectId:
    with open(file_path, 'rb') as file:
        id = fs.put(file, filename=os.path.basename(file_path))
        print(f"File stored with id: {id}")
        return id


def download(fs: GridFS, id: ObjectId, output_file_path: str):
    with open(output_file_path, 'wb') as output_file:
        grid_fs_file = fs.get(id)
        output_file.write(grid_fs_file.read())
        print(f"File retrieved and saved to {output_file_path}")


with MongoClient(MONGO_ADDRESS) as client:
    mongo_db = client[MONGO_DB_NAME]
    grid_fs = GridFS(mongo_db, collection=GRID_FS_COLLECTION_NAME)
    # List GridFS file ids
    print([file._id for file in grid_fs.find()])
    # upload file to GridFS
    file_id = upload(grid_fs, INPUT_FILE_PATH)
    # download file from GridFS
    download(grid_fs, file_id, OUTPUT_FILE_PATH)
