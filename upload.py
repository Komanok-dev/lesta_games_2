import re

from fastapi import FastAPI, File, UploadFile, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from typing import Annotated

from database import DatabaseHandler

DISPLAY_ROWS = 50

app = FastAPI()
templates = Jinja2Templates(directory="templates")


@app.get('/', response_class=HTMLResponse)
async def root_page(request: Request) -> Annotated[HTMLResponse, "HTMLResponse"]:
    return templates.TemplateResponse('upload.html', {"request": request})


@app.post('/upload/')
async def upload_file(
    request: Request, file: UploadFile=File(...)
    ) -> Annotated[HTMLResponse, "HTMLResponse"]:
    '''
    This func connects to db, checks if file exists, reads words from file
    and inserts them into db with needed connections between tables.
    Then calculates idf and display table:
    word, tf, idf of last uploaded file sorted by idf descending up to 50 rows.
    
    '''

    db_handler = DatabaseHandler()
    await db_handler.connect()
    await db_handler.create_tables()

    if await db_handler.check_file_exists(file.filename):
        return f'File {file.filename} is already in database'
    
    file_content = (await file.read()).decode('utf-8')
    words = re.findall(r'\b[\w-]+\b', file_content)

    if len(words) < 1:
        return f'File {file.filename} has no words'
    
    words_cnt = {}
    for word in words:
        words_cnt[word.lower()] = words_cnt.get(word.lower(), 0) + 1
    
    await db_handler.insert_words(file.filename, words_cnt)

    data = []
    for word, tf in words_cnt.items():
        idf = await db_handler.calc_idf(word)
        data.append((idf, tf, word))
    data = sorted(data, reverse=True)[:DISPLAY_ROWS]
    return templates.TemplateResponse('table.html', {"request": request, 'data': data})
