import pymongo
import os

DATA_DIR = os.path.abspath("data")

def clean_non_ascii(content_list):
    """
    Return text cleaned of non-ascii characters

    Args:
    content_list - list

    output:
    list
    """
    return [t.encode('ascii', errors='ignore').decode() for t in content_list]

class NYTDocLoader:
    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client.nyt_dump
        self.collection = self.db.articles
        self.iterator = self.collection.find()
        self.n = self.collection.count_documents({})
        self.i = 0

    def __iter__(self):
        self.i = 0
        self.iterator = self.collection.find()
        return self

    def __next__(self):
        if self.i == self.n:
            raise StopIteration
        result = self.iterator.next()
        self.i += 1
        return result

def main():
    files = os.listdir(DATA_DIR)

    dl = NYTDocLoader()
    document_ids = []
    section_names = []
    n_files_written = 0
    for d in dl:
        id = d['_id']
        sn = d['section_name']
        document_ids.append(id)
        section_names.append(sn)
        if id + '.txt' not in files:
            content = ' '.join(clean_non_ascii(d['content']))
            with open(os.path.join(DATA_DIR,id + '.txt'), 'w') as f:
                f.write(content)
            n_files_written += 1
        else:
            pass
    if 'section_names.csv' not in files:
        with open(os.path.join(DATA_DIR, 'section_names.csv'), 'w') as g:
            for _ in zip(document_ids, section_names):
                g.write(",".join(_) + '\n')
        n_files_written += 1
    print("Number of files written: ", n_files_written)

if __name__ == '__main__':
    main()
