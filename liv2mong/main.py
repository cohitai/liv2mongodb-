import livingdoc2mongodb as Liv2Mong
import argparse
import logging
import sys
logging.basicConfig(stream=sys.stdout, filemode='a', level=logging.INFO)


def main():
    parser = argparse.ArgumentParser(description="MongoDB backend")
    parser.add_argument("-A", "--automate", help="automate server by time", nargs='+', type=int)
    parser.add_argument("-C", "--update", help="custum update", nargs='+', type=int)
    args = parser.parse_args()

    M2L = Liv2Mong.MongoLivingdocs()

    if args.automate:

        logging.info("starting automation.... ")
        M2L.automation(args.automate[0])

    if args.update:

        logging.info("starting updating.... ")
        M2L.update_articles(args.update[0])


if __name__ == "__main__":
    main()
