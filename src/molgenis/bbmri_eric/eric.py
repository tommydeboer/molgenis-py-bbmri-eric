import argparse
import logging
import sys
import textwrap

from molgenis.bbmri_eric import __version__, bbmri_client

_logger = logging.getLogger(__name__)

_description = textwrap.dedent(
    rf"""
          __       __   __         __
    |\/| /  \ |   / _  |_  |\ | | (_
    |  | \__/ |__ \__) |__ | \| | __)
 __   __        __         __  __     __    __
|__) |__) |\/| |__) |  __ |_  |__) | /     /   |   |
|__) |__) |  | | \  |     |__ | \  | \__   \__ |__ |     v{__version__}

example usage:
  # Stage data from all or some external national nodes to the directory:
  eric stage *
  eric stage nl de be

  # Publish all or some national nodes in the production directory:
  eric publish *
  eric publish nl de be uk
"""
)


def main(args):
    """Wrapper allowing functions to be called with string arguments in a CLI fashion

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--verbose", "42"]``).
    """
    args = parse_args(args)
    setup_logging(args.loglevel)
    bbmri_session = bbmri_client.BbmriSession(
        url=args.target,
        username=args.username,
        password=args.password,
    )

    bbmri_session.stage_all_external_nodes()
    bbmri_session.publish_all_nodes()


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )


def run():
    """Calls :func:`main` passing the CLI arguments extracted from :obj:`sys.argv`"""
    main(sys.argv[1:])


if __name__ == "__main__":
    run()


def parse_args(args):
    """Parse command line parameters

    Args:
      args (List[str]): command line parameters as list of strings
          (for example  ``["--help"]``).

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        usage=argparse.SUPPRESS,
        formatter_class=argparse.RawTextHelpFormatter,
        description=_description,
    )

    parser.add_argument(
        "action", choices=["stage", "publish"], help="action to perform on the nodes"
    )
    parser.add_argument(
        dest="nodes",
        help="one or more nodes to stage or publish (separated by whitespace)",
        type=str,
        nargs="+",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="molgenis-py-bbmri-eric {ver}".format(ver=__version__),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )
    return parser.parse_args(args)
