import { useEffect, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation, useParams, useSearchParams } from 'react-router-dom';
import type { InsightState } from '../../api/schema';
import { ScreenPage } from '../ScreenPage';
import { Breadcrumb, pipelineCrumb } from '../../common/Breadcrumb';
import { Segmented } from '../../common/Segmented';
import { EmptyState, QueryView } from '../../common/States';
import { useResource } from '../../common/useResource';
import { InsightsBoard } from './InsightsBoard';

// The canvas groups open and acknowledged insights as needing attention.
type Tab = 'attention' | 'resolved' | 'muted';
const tabOf = (state: InsightState): Tab =>
  state === 'resolved' ? 'resolved' : state === 'muted' ? 'muted' : 'attention';

// The insights of a pipeline (spec 11.1), by kind and step, each with its evidence: figures, sample items, a way
// in (metrics, dead letters) and Acknowledge or Mute. The Improve view shows them too, with Propose a fix.
export function InsightsScreen() {
  const { t } = useTranslation();
  const { pipeline = '' } = useParams();
  const [search, setSearch] = useSearchParams();
  const insights = useResource(`insights:${pipeline}`, (api) => api.listInsights({ pipeline }));
  // A link to one insight (#id, from a lesson) opens on its tab, scrolled to its card and focused.
  const { hash } = useLocation();
  const target = hash.length > 1 ? decodeURIComponent(hash.slice(1)) : null;
  const targeted = insights.status === 'ready' ? insights.data.find((i) => i.id === target) : undefined;
  const tab =
    (['attention', 'resolved', 'muted'] as const).find((x) => x === search.get('state')) ??
    (targeted ? tabOf(targeted.state) : 'attention');
  const focused = useRef<string | null>(null);
  useEffect(() => {
    if (targeted === undefined || focused.current === targeted.id) return;
    const card = document.getElementById(targeted.id);
    if (card === null) return;
    focused.current = targeted.id;
    card.scrollIntoView({ block: 'start' });
    card.focus();
  }, [targeted]);

  const all = insights.status === 'ready' ? insights.data : [];
  const counts = { attention: 0, resolved: 0, muted: 0 };
  for (const i of all) counts[tabOf(i.state)] += 1;
  const inTab = all.filter((i) => tabOf(i.state) === tab);

  return (
    <ScreenPage
      title={t('nav.insights')}
      context={<Breadcrumb items={[pipelineCrumb(pipeline)]} />}
      help={t('insights.explain')}
      toolbar={
        <Segmented<Tab>
          label={t('insights.stateLabel')}
          value={tab}
          options={[
            { value: 'attention', label: t('insights.tab.attention', { count: counts.attention }) },
            { value: 'resolved', label: t('insights.tab.resolved', { count: counts.resolved }) },
            { value: 'muted', label: t('insights.tab.muted', { count: counts.muted }) },
          ]}
          onChange={(value) => setSearch(value === 'attention' ? {} : { state: value })}
        />
      }
    >
      <QueryView query={insights}>
        {() => (
          <InsightsBoard
            insights={inTab}
            onChanged={insights.refresh}
            empty={
              <EmptyState
                title={tab === 'attention' ? t('insights.none') : t('insights.noneHere')}
                done={tab === 'attention'}
              >
                {tab === 'attention' && t('insights.noneBody')}
              </EmptyState>
            }
          />
        )}
      </QueryView>
    </ScreenPage>
  );
}
