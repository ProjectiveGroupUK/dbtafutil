import sys
import warnings
import argparse
from dbtafutil.utils.logger import Logger
from dbtafutil.utils.utils import ExitCodes   ##TBC
import dbtafutil.commands.gendag as genDagCommand

VERSION = '1.0.1'
logger = Logger().initialize('test')

class DAUVersion(argparse.Action):
    """This is very similar to the built-in argparse._Version action,
    except it just calls tips.version.get_version_information().
    """

    def __init__(
        self,
        option_strings,
        version=None,
        dest=argparse.SUPPRESS,
        default=argparse.SUPPRESS,
        help="show program's version number and exit",
    ):
        super().__init__(
            option_strings=option_strings, dest=dest, default=default, nargs=0, help=help
        )

    def __call__(self, parser, namespace, values, option_string=None):
        formatter = argparse.RawTextHelpFormatter(prog=parser.prog)
        formatter.add_text(f'Version: {VERSION}')
        parser.exit(message=formatter.format_help())


class DAUArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.register("action", "dauversion", DAUVersion)

    def add_optional_argument_inverse(
        self,
        name,
        *,
        enable_help=None,
        disable_help=None,
        dest=None,
        no_name=None,
        default=None,
    ):
        mutex_group = self.add_mutually_exclusive_group()
        if not name.startswith("--"):
            raise Exception(
                'cannot handle optional argument without "--" prefix: ' f'got "{name}"'
            )
        if dest is None:
            dest_name = name[2:].replace("-", "_")
        else:
            dest_name = dest

        if no_name is None:
            no_name = f"--no-{name[2:]}"

        mutex_group.add_argument(
            name,
            action="store_const",
            const=True,
            dest=dest_name,
            default=default,
            help=enable_help,
        )

        mutex_group.add_argument(
            f"--no-{name[2:]}",
            action="store_const",
            const=False,
            dest=dest_name,
            default=default,
            help=disable_help,
        )

        return mutex_group


def main(args=None):
    logger.debug(f'Inside main')
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="logbook")
    if args is None:
        args = sys.argv[1:]    

    # logger.info('dbfutil Process invoked..')


    try:
        results, succeeded = handle_and_check(args)
        if succeeded:
            exit_code = ExitCodes.Success.value
        else:
            exit_code = ExitCodes.ModelError.value

    except KeyboardInterrupt:
        # if the logger isn't configured yet, it will use the default logger
        exit_code = ExitCodes.UnhandledError.value

    # This can be thrown by eg. argparse
    except SystemExit as e:
        exit_code = e.code

    except BaseException as e:
        logger.error(e)
        exit_code = ExitCodes.UnhandledError.value

    sys.exit(exit_code)

def handle_and_check(args):
    logger.debug('Inside handle_and_check')
    parsed = parse_args(args)

    task, res = run_from_args(parsed)
    success = task.interpret_results(res)

    return res, success

def run_from_args(parsed):
    # this will convert DbtConfigErrors into RuntimeExceptions
    # task could be any one of the task objects
    task = parsed.cls.from_args(args=parsed)

    # Set up logging
    log_path = None
    if task.config is not None:
        log_path = getattr(task.config, "log_path", None)

    results = task.run()

    return task, results


def _build_base_subparser():
    logger.debug('Inside _build_base_subparser')
    base_subparser = argparse.ArgumentParser(add_help=False)

    base_subparser.set_defaults(defer=None, state=None)
    return base_subparser

def _build_gendag_subparser(subparsers, base_subparser):
    logger.debug('Inside _build_gendag_subparser')

    sub = subparsers.add_parser(
        "gendag",
        parents=[base_subparser],
        help="""
        Genereate Airflow Dags.
        """,
    )

    sub.add_argument(
        "-m",
        "--models",
        dest="models",
        nargs="+",
        help="""
        Specify the models to select for generation of dags.
        Mutually exclusive with '--select' (or '-s')
        """,
        metavar="MODEL",
        required=False,
    )

    sub.add_argument(
        "-s",
        "--select",
        dest="select",
        nargs="+",
        help="""
        Specify the models to select for generation of dags.
        Mutually exclusive with '--models' (or '-m')
        """,
        metavar="MODEL",
        required=False,
    )    

    sub.add_argument(
        "-t",
        "--tag",
        dest="tags",
        nargs="+",
        help="""
        Specify the tags assigned to models that should form 
        the basis for generation of dags.
        """,
        metavar="TAG",
        required=False,
    )  

    sub.add_argument(
        "-st",
        "--skip-tests",
        dest="skip_tests",
        action="store_true",
        help="""
        Skips database metadata setup, when this flag is included
        """,
    )

    sub.add_argument(
        "-o",
        "--dags-output-folder",
        dest="dags_output_folder",
        help="""
        Specify absolute path of output folder where  
        dags should be generated.
        """,
        metavar="FOLDER",
        required=False,
    )  

    sub.add_argument(
        "-p",
        "--dbt-project-folder",
        dest="dbt_project_folder",
        help="""
        Specify absolute path of dbt project folder  
        if different from current location.
        """,
        metavar="FOLDER",
        required=False,
    )  

    sub.set_defaults(cls=genDagCommand.GenDagTask, which="gendag", rpc_method=None)
    return sub


def parse_args(args, cls=DAUArgumentParser):
    logger.debug('Inside parse_args')
    
    p = cls(
        prog="dbtafutil",
        description="""
        dbtafutil -> dbt to Airflow Utilities:
        Utilities Package for dbt and Airflow.
        E.g. it can generate Airflow DAGs based on dbt pipelines
        """,
        epilog="""
        Specify one of these sub-commands and you can find more help from
        there.
        """,
    )

    p.add_argument(
        "--version",
        action="dauversion",
        help="""
        Show version information
        """,
    )

    subs = p.add_subparsers(title="Available sub-commands")

    base_subparser = _build_base_subparser()

    _build_gendag_subparser(subs, base_subparser)


    if len(args) == 0:
        p.print_help()
        sys.exit(1)

    parsed = p.parse_args(args)

    if not hasattr(parsed, "which"):
        p.print_help()
        p.exit(1)

    return parsed

if __name__ == "__main__":
    main()

