from flask import Flask, request

app = Flask(__name__)

@app.route('/dbspublish', methods=['POST'])
def publishInDBS3():
    """

    """
    content = request.json
    userDN = request.args.get("DN", "")

    return userDN   

if __name__ == '__main__':
    app.run(host= '0.0.0.0', port=8443,debug=True)
