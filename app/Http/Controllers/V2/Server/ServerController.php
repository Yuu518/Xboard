<?php

namespace App\Http\Controllers\V2\Server;

use App\Http\Controllers\Controller;
use App\Models\Server;
use App\Services\ServerService;
use App\WebSocket\NodeWorker;
use Illuminate\Http\Request;
use Illuminate\Http\JsonResponse;
use Illuminate\Support\Facades\Cache;
use Illuminate\Validation\ValidationException;
use Symfony\Component\HttpKernel\Exception\HttpExceptionInterface;
use Throwable;

class ServerController extends Controller
{
    /**
     * server handshake api
     */
    public function handshake(Request $request): JsonResponse
    {
        $websocket = ['enabled' => false];

        if ((bool) admin_setting('server_ws_enable', 1) && Cache::has(NodeWorker::HEARTBEAT_CACHE_KEY)) {
            $customUrl = trim((string) admin_setting('server_ws_url', ''));

            if ($customUrl !== '') {
                $wsUrl = rtrim($customUrl, '/');
            } else {
                $wsScheme = $request->isSecure() ? 'wss' : 'ws';
                $wsUrl = "{$wsScheme}://{$request->getHttpHost()}/ws";
            }

            $websocket = [
                'enabled' => true,
                'ws_url' => $wsUrl,
            ];
        }

        return response()->json([
            'websocket' => $websocket
        ]);
    }

    /**
     * node report api - merge traffic + alive + status + metrics
     */
    public function report(Request $request): JsonResponse
    {
        $node = $request->attributes->get('node_info');

        if ($request->boolean('report_parts')) {
            $results = [];
            if ($request->hasAny(['traffic', 'report_stream', 'report_seq'])) {
                $results['traffic'] = $this->reportPart(fn() => $this->reportTraffic($request, $node, true));
            }
            if ($request->hasAny(['alive', 'online', 'status', 'metrics'])) {
                $results['status'] = $this->reportPart(function () use ($request, $node) {
                    $request->validate([
                        'alive' => 'sometimes|array',
                        'online' => 'sometimes|array',
                        'status' => 'sometimes|array',
                        'metrics' => 'sometimes|array',
                    ]);
                    ServerService::touchNode($node);
                    $this->reportStatus($request, $node);
                    return [];
                });
            }
            return response()->json(['results' => (object) $results]);
        }

        ServerService::touchNode($node);
        $ack = $this->reportTraffic($request, $node);
        $this->reportStatus($request, $node);
        return response()->json(['data' => true] + $ack);
    }

    private function reportPart(callable $process): array
    {
        try {
            return ['ok' => true] + $process();
        } catch (ValidationException $e) {
            return ['ok' => false, 'code' => $e->status];
        } catch (HttpExceptionInterface $e) {
            return ['ok' => false, 'code' => $e->getStatusCode()];
        } catch (Throwable $e) {
            report($e);
            return ['ok' => false, 'code' => 500];
        }
    }

    private function reportTraffic(Request $request, Server $node, bool $requireBatch = false): array
    {
        $traffic = $request->input('traffic');
        if ($requireBatch || $request->hasAny(['report_stream', 'report_seq'])) {
            $batch = $request->validate([
                'report_stream' => 'required|uuid',
                'report_seq' => 'required|integer|min:1',
                'traffic' => 'required|array|min:1',
                'traffic.*' => 'required|array|size:2',
                'traffic.*.0' => 'required|integer|min:0',
                'traffic.*.1' => 'required|integer|min:0',
            ]);
            ServerService::processTrafficReport($node, $traffic, $batch['report_stream'], (int) $batch['report_seq']);
            return ['report_stream' => $batch['report_stream'], 'report_seq' => (int) $batch['report_seq']];
        } elseif (is_array($traffic) && !empty($traffic)) {
            ServerService::processTraffic($node, $traffic);
        }

        return [];
    }

    private function reportStatus(Request $request, Server $node): void
    {
        $alive = $request->input('alive');
        if (is_array($alive) && !empty($alive)) {
            ServerService::processAlive($node->id, $alive);
        }

        $online = $request->input('online');
        if (is_array($online) && !empty($online)) {
            ServerService::processOnline($node, $online);
        }

        $status = $request->input('status');
        if (is_array($status) && !empty($status)) {
            ServerService::processStatus($node, $status);
        }

        $metrics = $request->input('metrics');
        if (is_array($metrics) && !empty($metrics)) {
            ServerService::updateMetrics($node, $metrics);
        }
    }
}
