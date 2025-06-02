import os
import signal
import sys
from flask import Flask, request
from pcomp.neuron import Neuron

# ─── Configuration from ENV ──────────────────────────────────────────────────
PARAMETERS   = os.environ["PARAMETERS"]    
KAFKA_BROKER = os.environ["KAFKA_BROKER"] 
REDIS_URL    = os.environ["REDIS_URL"]
BATCH_SIZE   = int(os.environ.get("BATCH_SIZE", "50"))
PORT         = int(os.environ.get("PORT", "5000"))


app = Flask(__name__)

# ─── Start Neuron thread on container startup ───────────────────────────
neuron = Neuron(PARAMETERS, KAFKA_BROKER, REDIS_URL)
neuron.daemon = True
neuron.start()

# ─── Graceful shutdown handler ────────────────────────────────────────────────
def handle_shutdown(signum, frame):
    try:
        neuron.close()
    except Exception:
        pass
    sys.exit(0)

# Catch Knative’s SIGTERM and SIGINT
signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT,  handle_shutdown)

# ─── HTTP endpoint for Knative Eventing & health checks ──────────────────────
@app.route("/", methods=["POST", "GET"])
def health_or_event():
    if request.method == "POST":
        # Acknowledge receipt of the CloudEvent
        return ("", 204)
    # Allow a simple GET for liveness probes
    return ("alive", 200)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
