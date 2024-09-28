import { useState, useEffect, useRef } from "react";
import { clone } from "lodash-es";
import { transformErrors, getPercent } from "../utills";
import { Profile, StatisticsDesc, StatisticsData, AttributeData, IOverview, MessageResponse } from "../types/ProfileGraphDashboard";

const CPU_TIEM_KEY = "CpuTime";
const WAIT_TIEM_KEY = "WaitTime";

export function useProfileData(): {
  plainData: Profile[];
  rangeData: Profile[];
  statisticsData: StatisticsData[];
  labels: AttributeData[];
  overviewInfo: IOverview | undefined;
  setOverviewInfo: React.Dispatch<React.SetStateAction<IOverview | undefined>>;
  isLoading: boolean;
  setIsLoading: React.Dispatch<React.SetStateAction<boolean>>;
  overviewInfoCurrent: React.RefObject<IOverview | undefined>;
} {
  const [plainData, setPlainData] = useState<Profile[]>([]);
  const [rangeData, setRangeData] = useState<Profile[]>([]);
  const [statisticsData, setStatisticsData] = useState<StatisticsData[]>([]);
  const [labels, setLabels] = useState<AttributeData[]>([]);
  const [overviewInfo, setOverviewInfo] = useState<IOverview|undefined>(undefined);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const overviewInfoCurrent = useRef<IOverview | undefined>(undefined);

  useEffect(() => {
    const fetchMessage = async () => {
      try {
        const response: Response = await fetch("/api/message");
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        const result: MessageResponse = await response.json();

        const data = JSON.parse(result?.result);

        const profiles = transformProfiles(data.profiles, data.statistics_desc);
        const overviewInfo = calculateOverviewInfo(profiles, data.statistics_desc);

        setPlainData(profiles);
        setRangeData(getRangeData(profiles));
        setOverviewInfo(overviewInfo);
        overviewInfoCurrent.current = overviewInfo;

        setStatisticsData(getStatisticsData(data.profiles, data.statistics_desc) as StatisticsData[]);
        setLabels(getLabels(data.profiles) as AttributeData[]);
      } catch (error) {
        console.error("Error fetching message:", error);
      } finally {
        setIsLoading(false);
      }
    };

    setIsLoading(true);
    fetchMessage();
  }, []);

  function transformProfiles(profiles: Profile[], statistics_desc: StatisticsDesc) {

    const cpuTimeIndex = statistics_desc[CPU_TIEM_KEY]?.index;
    const waitTimeIndex = statistics_desc[WAIT_TIEM_KEY]?.index;
    let cpuTime = 0;
    let waitTime = 0;

    profiles.forEach(item => {
      item.id = String(item.id);
      item.parent_id = String(item.parent_id);
      const cpuT = item?.statistics[cpuTimeIndex] || 0;
      const waitT = item?.statistics[waitTimeIndex] || 0;
      item.totalTime = cpuT + waitT;
      item.cpuTime = cpuT;
      item.waitTime = waitT;
      cpuTime += cpuT;
      waitTime += waitT;
      item.errors = item?.errors?.length > 0 ? transformErrors(item?.errors) : [];
      item.statisticsDescArray = createStatisticsDescArray(item, statistics_desc);
    });

    const totalTime = cpuTime + waitTime;
    profiles.forEach(item => {
      item.totalTimePercent = getPercent(item?.totalTime, totalTime);
      item.cpuTimePercent = getPercent(item?.cpuTime, item.totalTime);
      item.waitTimePercent = getPercent(item?.waitTime, item.totalTime);
    });

    return profiles;
  }

  function createStatisticsDescArray(item: Profile, statistics_desc: StatisticsDesc) {
    return Object.entries(statistics_desc).map(
      ([_type, descObj]) => ({
        _type,
        desc: descObj?.desc,
        display_name: descObj?.display_name || descObj?.displayName,
        index: descObj?.index,
        unit: descObj.unit,
        plain_statistics: descObj?.plain_statistics,
        _value: item.statistics[descObj?.index],
      })
    );
}

  function calculateOverviewInfo(profiles: Profile[], statistics_desc: StatisticsDesc) {
    const cpuTime = profiles.reduce((sum: number, item: Profile) => sum + item.cpuTime, 0);
    const waitTime = profiles.reduce((sum: number, item: Profile) => sum + item.waitTime, 0);
    const totalTime = cpuTime + waitTime;
    const cpuTimePercent = getPercent(cpuTime, totalTime);
    const waitTimePercent = getPercent(waitTime, totalTime);

    return {
      cpuTime,
      waitTime,
      totalTime,
      totalTimePercent: "100%",
      cpuTimePercent,
      waitTimePercent,
      statisticsDescArray: [],
      errors: [],
    };
  }

  function getRangeData(profiles: Profile[]) {
    return clone(profiles)
      ?.filter(item => parseFloat(item.totalTimePercent) > 0)
      ?.sort((a, b) => b.totalTime - a.totalTime);
  }

  function getStatisticsData(profiles: Profile[], statistics_desc: StatisticsDesc) {
    return profiles.map(profile => {
      const statistics = Object.entries(statistics_desc).map(([key, value]) => ({
        name: value.display_name || key,
        desc: value.desc,
        value: profile.statistics[value.index],
        unit: value.unit,
      }));
      return { statistics, id: profile?.id?.toString() };
    });
  }

  function getLabels(profiles: Profile[]) {
    return profiles.map(profile => ({
      labels: profile.labels,
      id: profile?.id?.toString(),
    }));
  }

  return {
    plainData,
    rangeData,
    statisticsData,
    labels,
    overviewInfo,
    setOverviewInfo,
    isLoading,
    setIsLoading,
    overviewInfoCurrent,
  };
}