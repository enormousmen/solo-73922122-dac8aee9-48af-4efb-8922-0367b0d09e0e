from . import app, init_app

init_app()
app.run(host="0.0.0.0", port=8000, threaded=True)
