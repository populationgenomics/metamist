if __name__ == '__main__':
    import os
    import uvicorn

    uvicorn.run(
        'api.main:app',
        host='0.0.0.0',
        port=int(os.getenv('PORT', '8000')),
        debug=True,
        reload=True,
    )
