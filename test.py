#!/usr/bin/python3
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# -*- coding: utf-8 -*-
import luigi
from luigi.mock import MockTarget


class Square(luigi.Task):
    number = luigi.IntParameter()

    def run(self):
        square = self.number * self.number
        with self.output().open("w") as fd:
            fd.write("{}\n".format(square))
        fd.close()

    def output(self):
        return MockTarget("square_{}".format(self.number))


class MainTask(luigi.Task):
    def run(self):
        data = []
        for number in range(0, 5):
            data.append(Square(number=number))
        yield data
        with self.output().open("w") as fd:
            fd.write("end")
        fd.close()

    def output(self):
        return MockTarget("MainTask")


if __name__ == '__main__':
    luigi.build(
        [
            MainTask()
        ],
        workers=4,
        parallel_scheduling=True,
        local_scheduler=True)
