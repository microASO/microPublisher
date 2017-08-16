from flask import Flask, request

app = Flask(__name__)

@app.route('/dbspublish', methods=['POST'])
def publishInDBS3():
    """

    """
    content = request.json
    userDN = request.args.get("DN", "")

    return userDN   