from flask import Blueprint, request, jsonify
import logging
from executors import get_agent_executor

blueprint = Blueprint('main', __name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

analysis_agent = get_agent_executor()

@blueprint.route('/api/chat', methods=['POST'])
def index():
    data = request.json
    response = {
        "data": None,
        "error": None
    }
    status_code = 404
    
    try:
        logging.info(f"Data: {data}")
        message = data['message']
        answer = analysis_agent.run(message)

        logging.info(f"Response: {answer}")
        response["data"] = answer
        status_code = 200
    
    except Exception as error:
        logging.error(error)
        response["error"] = {'message': f"{error}"}
    
    return jsonify(response), status_code
