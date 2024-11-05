// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app::schema as mt;
use fastrace::func_name;

use crate::common;

#[test]
fn test_decode_v65_least_visible_time() -> anyhow::Result<()> {
    let bytes: Vec<u8> = vec![
        10, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 50, 58, 53, 49, 58, 48, 55, 32, 85,
        84, 67, 160, 6, 64, 168, 6, 24,
    ];

    let want = || mt::LeastVisibleTime {
        time: DateTime::<Utc>::from_timestamp(10267, 0).unwrap(),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 64, want())?;

    Ok(())
}
