import argparse
import logging
import signal
import sys
import textwrap
from getpass import getpass
from typing import List, Tuple

import requests

from molgenis.bbmri_eric import __version__, bbmri_client
from molgenis.bbmri_eric.bbmri_client import EricSession
from molgenis.bbmri_eric.eric import Eric
from molgenis.client import MolgenisRequestError

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
  eric stage all
  eric stage nl de be

  # Publish all or some national nodes to the production tables:
  eric publish all
  eric publish nl de be uk
"""
)


def main(args: List[str]):
    """Parses the command line arguments and calls the corresponding actions.

    Args:
      args (List[str]): command line parameters as list of strings
    """
    signal.signal(signal.SIGINT, interrupt_handler)
    args = parse_args(args)

    try:
        session = _create_session(args)
        eric = Eric(session)
        execute_command(args, eric)
    except MolgenisRequestError as e:
        print(e.message)
        exit(1)
    except requests.RequestException as e:
        print(str(e))
        exit(1)


def _create_session(args) -> EricSession:
    username, password = _get_username_password(args)
    session = bbmri_client.EricSession(url=args.target)
    session.login(username, password)
    return session


def _get_username_password(args) -> Tuple[str, str]:
    if not args.username:
        username = input("Username: ")
    else:
        username = args.username
    password = getpass()
    return username, password


def execute_command(args, eric: Eric):
    all_nodes = len(args.nodes) == 1 and args.nodes[0] == "all"
    if args.action == "stage":
        try:
            if all_nodes:
                nodes = eric.session.get_external_nodes()
            else:
                codes = [code.upper() for code in args.nodes]
                nodes = eric.session.get_external_nodes(codes)
        except KeyError as e:
            eric.printer.print(str(e))
        else:
            eric.stage_external_nodes(nodes)
    elif args.action == "publish":
        try:
            if all_nodes:
                nodes = eric.session.get_nodes()
            else:
                codes = [code.upper() for code in args.nodes]
                nodes = eric.session.get_nodes(codes)
        except KeyError as e:
            eric.printer.print(str(e))
        else:
            eric.publish_nodes(nodes)
    else:
        raise ValueError("Unknown command")


def run():
    """Calls :func:`main` passing the CLI arguments extracted from :obj:`sys.argv`"""
    main(sys.argv[1:])


# noinspection PyUnusedLocal
def interrupt_handler(sig, frame):
    """
    Prints a friendly message instead of a traceback if the program is
    interrupted/stopped by a user.
    """
    print("Interrupted by user")
    sys.exit(0)


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
        help="one or more nodes to stage or publish (separated by whitespace) - "
        "use 'all' to select all nodes",
        type=str,
        nargs="+",
    )
    parser.add_argument(
        "--target",
        "-t",
        help="the URL of the target directory (default: "
        "https://directory.bbmri-eric.eu/)",
        default="https://directory.bbmri-eric.eu/",
    )
    parser.add_argument(
        "--username",
        "-u",
        help="the username to use when connecting to the target (will be prompted if "
        "not provided)",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="molgenis-py-bbmri-eric {ver}".format(ver=__version__),
    )
    return parser.parse_args(args)


if __name__ == "__main__":
    run()
