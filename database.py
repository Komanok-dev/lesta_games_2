import math
import sqlite3


class DatabaseHandler:
    def __init__(self, db_name='words.db') -> None:
        self.db_name = db_name
        self.connection = None
        self.cursor = None
        self.tables = [
            '''CREATE TABLE IF NOT EXISTS Words (
                id INTEGER PRIMARY KEY,
                name TEXT
            );''',
            '''CREATE TABLE IF NOT EXISTS Files (
                id INTEGER PRIMARY KEY,
                name TEXT
            );''',
            '''CREATE TABLE IF NOT EXISTS WordsToFiles (
                word_id INTEGER,
                file_id INTEGER,
                count INTEGER,
                FOREIGN KEY (word_id) REFERENCES Words(id),
                FOREIGN KEY (file_id) REFERENCES Files(id)
            );'''
        ]

    def connect(self) -> None:
        self.connection = sqlite3.connect(self.db_name)
        self.cursor = self.connection.cursor()


    def create_tables(self) -> None:
        for table in self.tables:
            self.cursor.execute(table)
        self.connection.commit()


    def check_file_exists(self, file: str) -> bool:
        '''Check if the file with the same name already exists in database.'''
        self.cursor.execute('SELECT * FROM Files WHERE name = ?', (file,))
        return True if self.cursor.fetchone() else False


    def check_word_exists(self, word: dict) -> None:
        '''Check if word already exists in database.'''
        self.cursor.execute('SELECT id FROM Words WHERE name = ?', (word,))
        row = self.cursor.fetchone()
        return row[0] if row else False


    def insert_words(self, file: str, words: dict) -> None:
        '''Insert words into the database.'''
        self.cursor.execute('INSERT INTO Files (name) VALUES (?)', (file,))
        file_id = self.cursor.lastrowid
        for word, cnt in words.items():
            word_id = self.check_word_exists(word)
            if not word_id:
                self.cursor.execute('INSERT INTO Words (name) VALUES (?)', (word,))
                word_id = self.cursor.lastrowid
            self.cursor.execute(
                'INSERT INTO WordsToFiles (word_id, file_id, count) VALUES (?, ?, ?)',
                (word_id, file_id, cnt)
            )
        self.connection.commit()


    def calc_idf(self, word: str) -> float:
        self.cursor.execute('SELECT COUNT(*) FROM Files')
        total_docs = self.cursor.fetchone()[0]
        self.cursor.execute(
            '''SELECT COUNT(DISTINCT file_id)
                FROM WordsToFiles
                WHERE word_id = (
                    SELECT id FROM Words WHERE name = ?
                )
            ''', (word,)
        )
        word_docs = self.cursor.fetchone()[0]
        return math.log(total_docs / word_docs) if word_docs > 0 else 0
