# coding: utf-8

from collections import namedtuple
from collections import defaultdict
import math

from sortedcontainers import SortedList
import click
from beautifultable import BeautifulTable


results_table_headers = ['ExecTime', 'TransNo', 'Weight,%', 'Percent']

fields = ['time', 'event', 'callcnt', 'fillcnt',
          'avgsize', 'maxsize', 'avgfull', 'maxfull',
          'minfull', 'avgdll', 'maxdll', 'avgtrip',
          'maxtrip', 'avgteap', 'maxteap', 'avgtsmr',
          'maxtsmr', 'mintsmr']

allowed_events = ['ORDER']

Transaction = namedtuple('Transaction', fields, defaults=(None,) * len(fields))
Event = namedtuple('Event', [
    'eventname', 'min', 'median', 'ninety', 'ninety_nine',
    'ninety_nine_dot_nine'])


class StatisticAnalyzer(object):

    def __init__(self, file_path, outfile_path, step):
        self.file_path = file_path
        self.outfile_path = outfile_path
        self.event_types = defaultdict(lambda: SortedList())
        self.event_stats = {}
        self.table_step = step

    def run(self):
        print('Parsing file..')
        self._parse_file()
        self._process_arrays()
        print('Writing results to: %s..' % self.outfile_path)
        self._output_to_file()
        print('Finished.')

    def _parse_file(self):
        """
        Метод заполнения необходимых полей из файла (если они есть)
         в сортированный словарь.
         Корректные типы операций содержатся в списке allowed_events.
        :return:
        """
        with open(self.file_path, 'r') as file:
            lines = 0
            for line in file:
                transaction = Transaction(*[i.strip('\n') for i in
                                            line.split('\t')])
                lines += 1
                if transaction.event not in allowed_events:
                    # Обрабатываем только корректные типы операций
                    continue
                try:
                    self.event_types[transaction.event].add(
                        int(transaction.avgtsmr))
                except ValueError:
                    # Значение не может быть преобразовано в число
                    pass
                except TypeError:
                    # Отсутствует значение в файле
                    pass

    def _process_arrays(self):
        """
        Заполение информации о минимальном значении
        для каждого типа операции, медиане и др.
        :return:
        """
        for event in self.event_types:
            print('Processing event: %s' % event)
            self.event_stats[event] = Event(
                eventname=event,
                min=self.event_types[event][0],
                median=self.event_types[event][
                    math.floor(len(self.event_types[event]) / 2)],
                ninety=self.event_types[event][
                    math.floor(len(self.event_types[event]) * 0.9)],
                ninety_nine=self.event_types[event][
                    math.floor(len(self.event_types[event]) * 0.99)],
                ninety_nine_dot_nine=self.event_types[event][
                    math.floor(len(self.event_types[event]) * 0.999)]
            )

    def _output_to_file(self):
        """
        Вывод результатов в файл, построение таблицы
        с результатом для каждого типа операций
        :return:
        """
        with open(self.outfile_path, 'w') as file:
            for event in self.event_types:
                file.write('Results for EVENTNAME: %s\n\n' % event)

                file.write('%s min=%s, 50%%=%s, 90%%=%s 99%%=%s 99.9%%=%s\n'
                           % (
                               self.event_stats[event].eventname,
                               self.event_stats[event].min,
                               self.event_stats[event].median,
                               self.event_stats[event].ninety,
                               self.event_stats[event].ninety_nine,
                               self.event_stats[event].ninety_nine_dot_nine
                           ))

                table = BeautifulTable()
                table.set_style(BeautifulTable.STYLE_BOX)
                step = self.table_step
                table.numeric_precision = 5
                table.column_headers = results_table_headers
                for header in results_table_headers:
                    table.column_alignments[header] = BeautifulTable.ALIGN_RIGHT
                for response_time in range(
                        0, self.event_types[event][-1] + step, step):
                    count = self.event_types[event].count(response_time)
                    if count != 0:
                        # скорее всего, под значением в последнем столбце
                        # имеется в виду процентная группа - "percentile"
                        table.append_row(
                            [
                                response_time,
                                count,
                                count * 100 / len(self.event_types[event]),
                                ((self.event_types[event].bisect_right(
                                    response_time) - 1) /
                                 len(self.event_types[event]) * 100)
                            ]
                        )
                file.write(str(table))
                file.write('\n')


@click.command()
@click.option('--in-file', help='Path to file', required=True)
@click.option('--out-file', help='Path to an output file',
              default='result.txt')
@click.option('--step', help='Step for results table', default=5)
def main(in_file, out_file, step):
    f = StatisticAnalyzer(in_file, out_file, step)
    f.run()


if __name__ == '__main__':
    main()
