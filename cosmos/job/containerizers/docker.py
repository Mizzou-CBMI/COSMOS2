from cosmos.job.containerizers.base import Containerizer


class Docker(Containerizer):

    """Represents the Docker containerizer: https://www.docker.com"""

    name = 'docker'
    required_arguments = {'image_tag'}
    containerizer_template = 'docker run {image_tag} "{cmd}"'
