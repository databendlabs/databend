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
fn test_decode_v152_vacuum_retention() -> anyhow::Result<()> {
    let want = || mt::VacuumRetention {
        time: DateTime::<Utc>::from_timestamp(0, 0).unwrap(),
        updated_by: "system".to_string(),
        updated_at: DateTime::<Utc>::from_timestamp(0, 0).unwrap(),
        version: 1,
    };

    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}