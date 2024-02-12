import json
import os
import argparse

def main(args) -> None:
    # Get all files in the current directory
    for filename in os.listdir(os.getcwd()):
        if filename.endswith('.json'):
            with open(os.path.join(os.getcwd(), filename), 'r+') as f:
                data = json.load(f)
                # Modify JSON fields
                if args.cloud != None:
                    data['env']['cloud'] = args.cloud
                if args.project != None:
                    data['env']['project'] = args.project
                if args.zone != None:
                    data['env']['zone'] = args.zone
                if args.user != None:
                    data['env']['user'] = args.user

                # Overwrite file contents
                f.seek(0)
                f.write(json.dumps(data, indent=4))
                f.truncate()
                print(f'Modified {filename}.')


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument('--cloud',
                        type=str,
                        help='local or gcp')
    parser.add_argument('--project',
                        type=str,
                        help='name of your gcp project, such as "bigger-not-badder"')
    parser.add_argument('--zone',
                        type=str,
                        help='the availability zone your nodes are in, such as "us-west1-b"')
    parser.add_argument('--user',
                        type=str,
                        help='your username on the remote machines, such as "davidchu"')

    return parser


if __name__ == '__main__':
    parser = get_parser()
    main(parser.parse_args())