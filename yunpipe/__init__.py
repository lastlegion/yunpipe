import os.path

from .utils import get_full_path
from .utils import create_folder

CLOUD_PIPE_TMP_FOLDER = get_full_path('~/.cloud_pipe/tmp')
CLOUD_PIPE_ALGORITHM_FOLDER = get_full_path('~/.cloud_pipe/clTools')
YUNPIPE_WORKFLOW_FOLDER = get_full_path('~/.cloud_pipe/workflow')
CLOUD_PIPE_TEMPLATES_FOLDER = os.path.join(os.path.dirname(__file__), 'templates')

create_folder(CLOUD_PIPE_TMP_FOLDER)
create_folder(CLOUD_PIPE_ALGORITHM_FOLDER)
create_folder(YUNPIPE_WORKFLOW_FOLDER)
