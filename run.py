from flask import Flask
import threading
from app.spark_job import start_spark_stream
from app.routes import routes_bp

app = Flask(__name__)
app.register_blueprint(routes_bp)

def start_spark_once():
    spark_thread = threading.Thread(target=start_spark_stream, daemon=True)
    spark_thread.start()

if __name__ == "__main__":
    start_spark_once()
    app.run(debug=False, use_reloader=False)

