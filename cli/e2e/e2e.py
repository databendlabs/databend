import time
import warnings

from absl.testing import absltest
from absl.testing import parameterized
import asynctest
import logging
import asyncio


async def execute(argument):
    proc = await asyncio.create_subprocess_exec(
        *argument,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)

    outdata = await proc.stdout.readline()
    out = outdata.decode('ascii').strip()

    errdata = await proc.stderr.readline()
    err = errdata.decode('ascii').strip()
    # Wait for the subprocess exit.
    await proc.wait()
    return out, err
class DatafuseCliTest(parameterized.TestCase, asynctest.TestCase):
    @parameterized.parameters(
        {'command': "10"},
        {'command': "--balala"},
        {'command': "foo"},
    )
    async def testUnsupportedCommands(self, command):
        out, err = await execute(("./datafuse-cli", command))
        self.assertTrue('''error: Found argument '{}' which wasn't expected, or isn't valid in this context'''.format(command) in err)


if __name__ == '__main__':
    absltest.main()