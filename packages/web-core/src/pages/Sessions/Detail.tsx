

import { useEffect, useState, useRef, useCallback } from 'react';
import { usePage, router, Link } from '@inertiajs/react';
import { Pause, Play, Archive, Tag, X, AlertCircle } from 'lucide-react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import { TelemetryFrame } from '@/types/telemetry';
import { Breadcrumbs } from '@/components/Breadcrumbs';
import { decodeMessage, createStopDemoMessage, createPingMessage } from '@/lib/telemetry-proto-browser';
import type { AnalysisLayoutJSON } from '@/lib/analysisLayout';
import { DEFAULT_ANALYSIS_LAYOUT } from '@/lib/analysisLayout';
import { formatLapTime } from '@/lib/utils';
import { useAuth } from '@/components/AuthProvider';
import { TelemetryDataPanel } from '@/components/TelemetryDataPanel';
import { queryKeys } from '@/lib/queryKeys';
import { fetchJson, mutationJson } from '@/lib/client-fetch';
import { useSessionUiStore } from '@/stores/sessionUiStore';
import { useLiveLapIndexStore } from '@/stores/liveLapIndexStore';
import { useNavUiStore } from '@/stores/navUiStore';
import { Session, LapSummary, DEFAULT_SESSION_TAGS } from '@/types/core';

export default function SessionDetailPage() {
  const { id: sessionId } = usePage().props as unknown as { id: string };
  const { user } = useAuth();

  const queryClient = useQueryClient();

  const getIsPaused = useSessionUiStore((s) => s.getIsPaused);
  const setIsPausedPersisted = useSessionUiStore((s) => s.setIsPaused);

  const sessionQuery = useQuery({
    queryKey: queryKeys.sessionDetail(sessionId),
    queryFn: async (): Promise<Session> => {
      return fetchJson<Session>(`/api/sessions/${sessionId}`, {
        unauthorized: { kind: 'redirect_to_login' },
      });
    },
    enabled: !!sessionId,
    refetchInterval: false,
  });

  const [session, setSession] = useState<Session | null>(null);
  const [loading, setLoading] = useState(true);
  const [isPaused, setIsPaused] = useState(() => getIsPaused(sessionId));
  const [currentLapFrames, setCurrentLapFrames] = useState<TelemetryFrame[]>([]);
  const [currentLapNumber, setCurrentLapNumber] = useState(1);
  const [wsConnected, setWsConnected] = useState(false);
  const [reconnecting, setReconnecting] = useState(false);
  const [sessionTags, setSessionTags] = useState<string[]>([]);
  const [customSessionTag, setCustomSessionTag] = useState('');
  const [showTagInput, setShowTagInput] = useState(false);
  const [analysisLayoutId, setAnalysisLayoutId] = useState<string | null>(null);
  const [analysisLayout, setAnalysisLayout] = useState<AnalysisLayoutJSON | null>(null);

  const liveSessionState = useLiveLapIndexStore((s) => s.bySessionId[sessionId]);
  const setDbLapSummaries = useLiveLapIndexStore((s) => s.setDbLapSummaries);
  const observeFrameLapNumber = useLiveLapIndexStore((s) => s.observeFrameLapNumber);

  const expandNode = useNavUiStore((s) => s.expandNode);

  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const reconnectAttemptsRef = useRef(0);
  const currentLapFramesRef = useRef<TelemetryFrame[]>([]);
  const currentLapNumberRef = useRef(1);
  const lastUpdateTimeRef = useRef(0);
  const isPausedRef = useRef(getIsPaused(sessionId));
  const isMountedRef = useRef(true);
  const heartbeatIntervalRef = useRef<NodeJS.Timeout | null>(null);
  const lastSavedTagsRef = useRef<string>('[]');

  const sessionLapsQuery = useQuery({
    queryKey: queryKeys.sessionLaps(sessionId),
    queryFn: async () => {
      return fetchJson<{ laps: LapSummary[] }>(
        `/api/laps?sessionId=${sessionId}`,
        { unauthorized: { kind: 'return_fallback' }, fallback: { laps: [] } },
      );
    },
    enabled: !!sessionId,
    staleTime: 15_000,
    refetchInterval: (query) => {
      const data = sessionQuery.data;
      return data?.status === 'active' ? 15_000 : false;
    },
  });

  useEffect(() => {
    const laps = sessionLapsQuery.data?.laps;
    if (!laps) return;
    setDbLapSummaries(sessionId, laps);
  }, [sessionId, sessionLapsQuery.data, setDbLapSummaries]);

  const completedLapCount = liveSessionState?.completedLapNumbers.length ?? 0;
  useEffect(() => {
    if (completedLapCount === 0) return;
    queryClient.invalidateQueries({ queryKey: queryKeys.sessionDetail(sessionId) });
    queryClient.invalidateQueries({ queryKey: queryKeys.navEvents });
    const timer = setTimeout(() => {
      queryClient.invalidateQueries({ queryKey: queryKeys.sessionDetail(sessionId) });
      queryClient.invalidateQueries({ queryKey: queryKeys.navEvents });
    }, 1500);
    return () => clearTimeout(timer);
  }, [completedLapCount, queryClient, sessionId]);

  const analysisLayoutsQuery = useQuery({
    queryKey: ['analysis-layouts', 'session', sessionId] as const,
    queryFn: async () => {
      const contextKey = `session:${sessionId}`;
      const data = await fetchJson<unknown>(`/api/analysis-layouts?context=${encodeURIComponent(contextKey)}`, {
        unauthorized: { kind: 'return_fallback' },
        fallback: [],
      });
      return Array.isArray(data) ? (data as any[]) : ([] as any[]);
    },
    enabled: !!sessionId,
  });

  const saveAnalysisLayoutMutation = useMutation({
    mutationFn: async (payload: { id: string; layout: AnalysisLayoutJSON }) => {
      await mutationJson(`/api/analysis-layouts/${payload.id}`, {
        method: 'PATCH',
        body: { layout: payload.layout },
      });
      return true as const;
    },
  });

  const startSessionMutation = useMutation({
    mutationFn: async () => {
      return mutationJson<Session>(`/api/sessions/${sessionId}/start`, {
        method: 'POST',
      });
    },
    onSuccess: (updated) => {
      queryClient.setQueryData(queryKeys.sessionDetail(sessionId), updated);
      isMountedRef.current = true;
      if (updated.event?.id) queryClient.invalidateQueries({ queryKey: queryKeys.eventDetail(updated.event.id) });
      queryClient.invalidateQueries({ queryKey: queryKeys.navEvents });
      setTimeout(() => {
        if (!wsRef.current && updated.status === 'active' && updated.started) {
          connectWebSocket();
        }
      }, 0);
    },
  });

  const patchSessionMutation = useMutation({
    mutationFn: async (payload: Record<string, any>) => {
      return mutationJson<Session>(`/api/sessions/${sessionId}`, {
        method: 'PATCH',
        body: payload,
      });
    },
    onSuccess: (updated) => {
      queryClient.setQueryData(queryKeys.sessionDetail(sessionId), updated);
      queryClient.invalidateQueries({ queryKey: queryKeys.sessionDetail(sessionId) });
      if (updated.event?.id) queryClient.invalidateQueries({ queryKey: queryKeys.eventDetail(updated.event.id) });
      queryClient.invalidateQueries({ queryKey: queryKeys.eventsList });
      queryClient.invalidateQueries({ queryKey: queryKeys.navEvents });
    },
  });

  const deleteSessionMutation = useMutation({
    mutationFn: async () => {
      return mutationJson<{ success: true }>(`/api/sessions/${sessionId}`, {
        method: 'DELETE',
      });
    },
    onSuccess: () => {
      queryClient.removeQueries({ queryKey: queryKeys.sessionDetail(sessionId) });
      queryClient.removeQueries({ queryKey: queryKeys.sessionLaps(sessionId) });
      if (session?.event?.id) {
        queryClient.invalidateQueries({ queryKey: queryKeys.eventDetail(session.event.id) });
      }
      queryClient.invalidateQueries({ queryKey: queryKeys.eventsList });
      queryClient.invalidateQueries({ queryKey: queryKeys.navEvents });
    },
  });

  useEffect(() => {
    currentLapFramesRef.current = [];
    setCurrentLapFrames([]);
    currentLapNumberRef.current = 1;
    setCurrentLapNumber(1);
    lastUpdateTimeRef.current = 0;
  }, [sessionId]);

  useEffect(() => {
    setIsPaused(getIsPaused(sessionId));
    isPausedRef.current = getIsPaused(sessionId);
  }, [getIsPaused, sessionId]);

  useEffect(() => {
    isPausedRef.current = isPaused;
    setIsPausedPersisted(sessionId, isPaused);
  }, [isPaused, sessionId, setIsPausedPersisted]);

  useEffect(() => {
    if (!sessionId) return;

    if (sessionQuery.isError) {
      console.error('Session not found or error loading session');
      alert('Session not found. It may have been deleted. Please create a new session.');
      router.visit('/');
      return;
    }

    if (!sessionQuery.data) {
      setLoading(true);
      return;
    }

    const data = sessionQuery.data;
    setSession(data);

    if (data.event?.id) {
      expandNode(`event:${data.event.id}`);
    }
    expandNode(`session:${sessionId}`);

    if (data.tags) {
      const newTags = JSON.parse(data.tags);
      setSessionTags((prev) => {
        if (JSON.stringify(prev) !== JSON.stringify(newTags)) {
          return newTags;
        }
        return prev;
      });
    }

    setLoading(false);
  }, [expandNode, sessionId, sessionQuery.data, sessionQuery.isError]);

  useEffect(() => {
    if (session && session.status === 'active' && session.started && !wsRef.current) {
      isPausedRef.current = isPaused;
      connectWebSocket();
    }
    return () => {
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }
      if (heartbeatIntervalRef.current) {
        clearInterval(heartbeatIntervalRef.current);
        heartbeatIntervalRef.current = null;
      }
    };
  }, [session?.status, session?.started, session?.source]);

  useEffect(() => {
    isMountedRef.current = true;
    return () => {
      isMountedRef.current = false;
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }
      if (heartbeatIntervalRef.current) {
        clearInterval(heartbeatIntervalRef.current);
        heartbeatIntervalRef.current = null;
      }
    };
  }, []);

  useEffect(() => {
    if (!session) return;
    const layouts = analysisLayoutsQuery.data ?? [];
    if (Array.isArray(layouts) && layouts.length > 0) {
      const layoutRecord = (layouts.find((l: any) => l.isDefault) ?? layouts[0]) as any;
      try {
        const parsed: AnalysisLayoutJSON = JSON.parse(layoutRecord.layout);
        const hasAnyType = parsed.panels.some((p) => !!p.typeId);
        const nextLayout = hasAnyType
          ? parsed
          : {
              ...parsed,
              panels: parsed.panels.map((p, idx) =>
                idx === 0 ? { ...p, typeId: 'plot' } : p,
              ),
            };
        setAnalysisLayoutId(layoutRecord.id);
        setAnalysisLayout(nextLayout);
        return;
      } catch (e) {
        console.error('Failed to parse saved analysis layout JSON for session:', e);
      }
    }
    setAnalysisLayoutId(null);
    setAnalysisLayout(DEFAULT_ANALYSIS_LAYOUT);
  }, [session, sessionId, analysisLayoutsQuery.data]);

  useEffect(() => {
    if (!session || !analysisLayout) return;
    if (!analysisLayoutId) return;
    saveAnalysisLayoutMutation.mutate({ id: analysisLayoutId, layout: analysisLayout });
  }, [analysisLayout, analysisLayoutId, session, sessionId]);

  function connectWebSocket() {
    if (wsRef.current) {
      wsRef.current.close();
      wsRef.current = null;
    }
    if (heartbeatIntervalRef.current) {
      clearInterval(heartbeatIntervalRef.current);
      heartbeatIntervalRef.current = null;
    }

    const userId = user?.id;
    if (!userId) {
      console.error('Cannot connect WebSocket: no authenticated user');
      return;
    }
    const ws = new WebSocket(`ws://localhost:8080?userId=${userId}&sessionId=${sessionId}`);
    ws.binaryType = 'arraybuffer';
    wsRef.current = ws;

    ws.onopen = () => {
      if (isMountedRef.current) {
        setWsConnected(true);
        setReconnecting(false);
      }
      reconnectAttemptsRef.current = 0;
      heartbeatIntervalRef.current = setInterval(() => {
        if (wsRef.current?.readyState === WebSocket.OPEN) {
          try {
            wsRef.current.send(createPingMessage());
          } catch (error) {
            console.error('Error sending heartbeat:', error);
          }
        }
      }, 30000);
    };

    ws.onmessage = (event) => {
      if (!isMountedRef.current) return;
      try {
        if (!(event.data instanceof ArrayBuffer)) return;
        const decoded = decodeMessage(event.data);
        handleDecodedMessage(decoded);
      } catch (error) {
        console.error('Error processing WebSocket message:', error);
      }
    };

    function handleDecodedMessage(message: any) {
      if (!isMountedRef.current) return;
      try {
        if (message.type === 'demo_complete') return;
        if (message.type === 'telemetry') {
          if (isPausedRef.current) return;
          const frame: TelemetryFrame = message.data;
          const frameLapNumber = frame.lapNumber;
          if (typeof frameLapNumber === 'number') {
            observeFrameLapNumber(sessionId, frameLapNumber);
          }
          const shouldStartNewLap =
            typeof frameLapNumber === 'number' && frameLapNumber !== currentLapNumberRef.current;
          if (shouldStartNewLap) {
            currentLapNumberRef.current = frameLapNumber;
            currentLapFramesRef.current = [frame];
            if (isMountedRef.current) {
              setCurrentLapNumber(frameLapNumber);
              setCurrentLapFrames([frame]);
            }
            lastUpdateTimeRef.current = Date.now();
            return;
          }
          currentLapFramesRef.current.push(frame);
          currentLapFramesRef.current.sort((a, b) => a.lapTime - b.lapTime);
          const now = Date.now();
          const isFirstFrame = currentLapFramesRef.current.length === 1;
          if ((isFirstFrame || now - lastUpdateTimeRef.current >= 67) && isMountedRef.current) {
            const buf = currentLapFramesRef.current;
            const LIVE_WINDOW = 2000;
            setCurrentLapFrames(buf.length > LIVE_WINDOW ? buf.slice(-LIVE_WINDOW) : [...buf]);
            lastUpdateTimeRef.current = now;
          }
        }
      } catch (error) {
        console.error('Error handling decoded message:', error);
      }
    }

    ws.onerror = () => {
      if (isMountedRef.current) setWsConnected(false);
    };

    ws.onclose = (event) => {
      if (heartbeatIntervalRef.current) {
        clearInterval(heartbeatIntervalRef.current);
        heartbeatIntervalRef.current = null;
      }
      if (isMountedRef.current) setWsConnected(false);
      wsRef.current = null;
      if (isMountedRef.current && session?.status === 'active' && !isPausedRef.current) {
        reconnectAttemptsRef.current++;
        const delay = Math.min(reconnectAttemptsRef.current * 1000, 5000);
        if (isMountedRef.current) setReconnecting(true);
        reconnectTimeoutRef.current = setTimeout(() => {
          if (isMountedRef.current && session?.status === 'active') {
            connectWebSocket();
          }
        }, delay);
      } else {
        if (isMountedRef.current) setReconnecting(false);
      }
    };
  }

  async function startSession() {
    try {
      const updatedSession = await startSessionMutation.mutateAsync();
      setSession(updatedSession);
      currentLapFramesRef.current = [];
      setCurrentLapFrames([]);
      currentLapNumberRef.current = 1;
      setCurrentLapNumber(1);
      lastUpdateTimeRef.current = 0;
    } catch (error) {
      console.error('Error starting session:', error);
    }
  }

  async function deleteSession() {
    const confirmed = window.confirm('Delete this session? This cannot be undone.');
    if (!confirmed) return;
    const returnTo = session?.event?.id ? `/event/${session.event.id}` : '/';
    router.visit(returnTo);
    try {
      deleteSessionMutation.mutate();
    } catch (error) {
      console.error('Error deleting session:', error);
    }
  }

  function togglePause() {
    const newPausedState = !isPaused;
    setIsPaused(newPausedState);
    isPausedRef.current = newPausedState;
    if (newPausedState) {
      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
        try {
          wsRef.current.send(createStopDemoMessage());
          setTimeout(() => {
            if (wsRef.current) {
              wsRef.current.close();
              wsRef.current = null;
            }
          }, 100);
        } catch (error) {
          if (wsRef.current) {
            wsRef.current.close();
            wsRef.current = null;
          }
        }
      }
      if (heartbeatIntervalRef.current) {
        clearInterval(heartbeatIntervalRef.current);
        heartbeatIntervalRef.current = null;
      }
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }
    } else {
      currentLapFramesRef.current = [];
      setCurrentLapFrames([]);
      currentLapNumberRef.current = currentLapNumber;
      if (session?.status === 'active') {
        connectWebSocket();
      }
    }
  }

  const autoSaveSessionTags = useCallback(async (tags: string[]) => {
    try {
      await patchSessionMutation.mutateAsync({ tags: JSON.stringify(tags) });
    } catch (error) {
      console.error('Error saving session tags:', error);
    }
  }, [patchSessionMutation]);

  useEffect(() => {
    if (!session) return;
    const tagsString = JSON.stringify(sessionTags);
    if (tagsString !== lastSavedTagsRef.current) {
      lastSavedTagsRef.current = tagsString;
      autoSaveSessionTags(sessionTags);
    }
  }, [sessionTags, session, autoSaveSessionTags]);

  function addSessionTag(tag: string) {
    if (!sessionTags.includes(tag)) {
      setSessionTags([...sessionTags, tag]);
    }
  }

  function removeSessionTag(tag: string) {
    setSessionTags(sessionTags.filter(t => t !== tag));
  }

  function addCustomSessionTag() {
    if (customSessionTag.trim() && !sessionTags.includes(customSessionTag.trim())) {
      setSessionTags([...sessionTags, customSessionTag.trim()]);
      setCustomSessionTag('');
      setShowTagInput(false);
    }
  }

  async function endSession() {
    try {
      isMountedRef.current = false;
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }
      if (heartbeatIntervalRef.current) {
        clearInterval(heartbeatIntervalRef.current);
        heartbeatIntervalRef.current = null;
      }
      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
        try {
          wsRef.current.send(createStopDemoMessage());
          await new Promise(resolve => setTimeout(resolve, 100));
        } catch (error) {
          console.error('Error sending stop_demo:', error);
        }
      }
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
      await patchSessionMutation.mutateAsync({ status: 'archived' });
      queryClient.invalidateQueries({ queryKey: queryKeys.sessionDetail(sessionId) });
      queryClient.invalidateQueries({ queryKey: queryKeys.navEvents });
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: queryKeys.sessionDetail(sessionId) });
        queryClient.invalidateQueries({ queryKey: queryKeys.navEvents });
      }, 2000);
      if (session?.event?.id) {
        router.visit(`/event/${session.event.id}`);
      } else {
        router.visit('/');
      }
    } catch (error) {
      console.error('Error ending session:', error);
    }
  }

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-purple-600 mx-auto"></div>
          <p className="mt-4 text-muted-foreground">Loading session...</p>
        </div>
      </div>
    );
  }

  if (!session) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <Card className="max-w-md">
          <CardHeader>
            <CardTitle>Session Not Found</CardTitle>
          </CardHeader>
          <CardContent>
            <Link href="/">
              <Button>Back to Sessions</Button>
            </Link>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-purple-50 to-blue-50 dark:from-gray-900 dark:to-gray-800">
      <header className="border-b bg-white dark:bg-gray-900 shadow-md sticky top-0 z-50">
        <div className="container mx-auto px-4 py-4">
          <div className="flex items-center justify-between">
            <div className="flex-1">
              {session.event && (
                <Breadcrumbs
                  items={[
                    { label: session.event.name, href: `/event/${session.event.id}` },
                    { label: session.name, href: `/session/${session.id}` },
                  ]}
                />
              )}
              <h1 className="text-xl font-bold mt-1">{session.name}</h1>
              <div className="flex items-center gap-2 mt-1">
                {session.status === 'active' ? (
                  <Badge variant={wsConnected ? 'default' : reconnecting ? 'secondary' : 'destructive'}>
                    {wsConnected ? 'Connected' : reconnecting ? 'Reconnecting...' : 'Disconnected'}
                  </Badge>
                ) : (
                  <Badge variant="secondary">Archived</Badge>
                )}
                <Badge variant="outline">{session.source}</Badge>
                {session.status === 'active' && (
                  <Badge variant="outline">Lap {currentLapNumber}</Badge>
                )}
              </div>

              <div className="flex flex-wrap items-center gap-1 mt-2">
                {sessionTags.map((tag) => (
                  <Badge
                    key={tag}
                    variant="secondary"
                    className="text-xs gap-1 cursor-pointer hover:bg-destructive"
                    onClick={() => removeSessionTag(tag)}
                  >
                    {tag}
                    <X className="h-2 w-2" />
                  </Badge>
                ))}
                {!showTagInput ? (
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-6 px-2 text-xs"
                    onClick={() => setShowTagInput(true)}
                  >
                    <Tag className="h-3 w-3 mr-1" />
                    Add Tag
                  </Button>
                ) : (
                  <div className="flex gap-1">
                    <div className="flex flex-wrap gap-1">
                      {DEFAULT_SESSION_TAGS.map((tag) => (
                        <Badge
                          key={tag}
                          variant={sessionTags.includes(tag) ? "default" : "outline"}
                          className="text-xs cursor-pointer"
                          onClick={() => sessionTags.includes(tag) ? removeSessionTag(tag) : addSessionTag(tag)}
                        >
                          {tag}
                        </Badge>
                      ))}
                    </div>
                    <div className="flex gap-1">
                      <Input
                        value={customSessionTag}
                        onChange={(e) => setCustomSessionTag(e.target.value)}
                        onKeyDown={(e) => e.key === 'Enter' && addCustomSessionTag()}
                        placeholder="Custom..."
                        className="h-6 w-24 text-xs"
                      />
                      <Button
                        size="sm"
                        variant="ghost"
                        className="h-6 px-2"
                        onClick={() => setShowTagInput(false)}
                      >
                        <X className="h-3 w-3" />
                      </Button>
                    </div>
                  </div>
                )}
              </div>
            </div>

            {(session.status === 'active' || session.status === 'pending') && (
              <div className="flex items-center gap-2">
                {!session.started && (
                  <Button
                    variant="default"
                    onClick={startSession}
                    className="gap-2"
                    title="Start telemetry collection for this session"
                  >
                    <Play className="h-4 w-4" />
                    Start Session
                  </Button>
                )}
                {session.started && session.status === 'active' && (
                  <Button
                    variant={isPaused ? 'default' : 'outline'}
                    onClick={togglePause}
                    className="gap-2"
                  >
                    {isPaused ? (
                      <>
                        <Play className="h-4 w-4" />
                        Resume
                      </>
                    ) : (
                      <>
                        <Pause className="h-4 w-4" />
                        Pause
                      </>
                    )}
                  </Button>
                )}
                <Button variant="destructive" onClick={endSession} className="gap-2">
                  <Archive className="h-4 w-4" />
                  End Session
                </Button>
              </div>
            )}

            {session.status !== 'active' && session.status !== 'pending' && (
              <div className="flex items-center gap-2">
                <Button
                  variant="destructive"
                  onClick={deleteSession}
                  className="gap-2"
                  disabled={deleteSessionMutation.isPending}
                >
                  Delete Session
                </Button>
              </div>
            )}
          </div>
        </div>
      </header>

      <main className="container mx-auto px-4 py-6">
        <div className={`grid grid-cols-1 ${session.status === 'active' || session.status === 'pending' ? 'lg:grid-cols-3' : 'lg:grid-cols-1'} gap-6`}>
          {(session.status === 'active' || session.status === 'pending') && (
            <div className="lg:col-span-2 space-y-6">
              {!session.started && (
                <Card className="border-blue-200 bg-blue-50 dark:bg-blue-950/30 dark:border-blue-800">
                  <CardContent className="pt-6">
                    <div className="flex items-start gap-3">
                      <AlertCircle className="h-5 w-5 text-blue-600 dark:text-blue-400 mt-0.5" />
                      <div>
                        <h3 className="font-semibold text-blue-900 dark:text-blue-100 mb-1">Session Not Started</h3>
                        <p className="text-sm text-blue-700 dark:text-blue-300">
                          This session has been pre-configured but telemetry collection hasn&apos;t started yet.
                          Click the <strong>&quot;Start Session&quot;</strong> button above to begin collecting telemetry data.
                        </p>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              )}

              {session.started && analysisLayout && (
                <TelemetryDataPanel
                  context="live"
                  uiScopeKey={`session:${sessionId}`}
                  telemetry={currentLapFrames}
                  layout={analysisLayout}
                  onLayoutChange={setAnalysisLayout}
                  hasSavedLayout={!!analysisLayoutId}
                />
              )}
            </div>
          )}

          <div className={session.status === 'active' ? 'space-y-4' : 'w-full max-w-6xl mx-auto'}>
            <Card>
              <CardHeader>
                <CardTitle>Completed Laps</CardTitle>
              </CardHeader>
              <CardContent>
                {(() => {
                  const isLive = session.status === 'active';
                  const storeState = liveSessionState;
                  const storeCompletedLapNumbers = storeState?.completedLapNumbers ?? [];
                  const laps = sessionLapsQuery.data?.laps ?? [];

                  const lapsByNumber = new Map<number, typeof laps[number]>();
                  for (const lap of laps) {
                    lapsByNumber.set(lap.lapNumber, lap);
                  }

                  const listLapNumbers = isLive
                    ? storeCompletedLapNumbers
                    : Array.from(new Set([
                        ...laps.map((l) => l.lapNumber),
                        ...storeCompletedLapNumbers,
                      ])).sort((a, b) => a - b);

                  const storeLapTimeByNumber = new Map<number, number | null>();
                  for (const s of storeState?.dbLapSummaries ?? []) {
                    storeLapTimeByNumber.set(s.lapNumber, s.lapTime ?? null);
                  }

                  if (listLapNumbers.length === 0) {
                    return (
                      <div className="text-sm text-muted-foreground">
                        No completed laps yet.
                      </div>
                    );
                  }

                  return (
                    <div className="space-y-2">
                      {listLapNumbers.map((lapNumber) => {
                        const persisted = lapsByNumber.get(lapNumber);
                        const lapTime = persisted?.lapTime ?? storeLapTimeByNumber.get(lapNumber) ?? null;
                        const lapTags = persisted?.tags ? JSON.parse(persisted.tags) : [];
                        const lapId = persisted?.id;
                        return (
                          <Link
                            key={lapNumber}
                            href={lapId ? `/lap/${lapId}` : '#'}
                            className="block"
                          >
                            <div className="p-3 rounded-lg border hover:bg-accent transition-colors cursor-pointer">
                              <div className="flex items-center justify-between">
                                <span className="font-semibold">Lap {lapNumber}</span>
                                <div className="flex items-center gap-2">
                                  {persisted?.analyzed && (
                                    <Badge variant="secondary" className="text-xs">Analyzed</Badge>
                                  )}
                                  <span className="text-sm text-muted-foreground">
                                    {lapTime != null ? formatLapTime(lapTime) : isLive ? 'Live' : '—'}
                                  </span>
                                </div>
                              </div>
                              {lapTags.length > 0 && (
                                <div className="flex flex-wrap gap-1 mt-2">
                                  {lapTags.map((tag: string) => (
                                    <Badge key={tag} variant="outline" className="text-xs">
                                      {tag}
                                    </Badge>
                                  ))}
                                </div>
                              )}
                            </div>
                          </Link>
                        );
                      })}
                    </div>
                  );
                })()}
              </CardContent>
            </Card>
          </div>
        </div>
      </main>
    </div>
  );
}
