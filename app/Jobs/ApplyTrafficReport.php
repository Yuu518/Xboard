<?php

namespace App\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Support\Facades\DB;

class ApplyTrafficReport implements ShouldQueue
{
    use Dispatchable, Queueable;

    public function __construct(private int $id)
    {
        $this->onQueue('traffic_fetch');
    }

    public static function store(array $server, array $traffic, string $protocol, int $timestamp): void
    {
        $id = DB::table('v2_server_traffic_pending')->insertGetId([
            'payload' => json_encode(compact('server', 'traffic', 'protocol', 'timestamp'), JSON_THROW_ON_ERROR),
            'created_at' => time(),
        ]);
        self::dispatch($id)->afterCommit();
    }

    public static function retryPending(): void
    {
        DB::table('v2_server_traffic_pending')->where('created_at', '<', time() - 300)
            ->select('id')->chunkById(100, function ($rows) {
                foreach ($rows as $row) {
                    self::dispatch($row->id);
                }
            });
    }

    public function handle(): void
    {
        DB::transaction(function () {
            $pending = DB::table('v2_server_traffic_pending')->where('id', $this->id);
            $raw = $pending->lockForUpdate()->value('payload');
            if ($raw === null) {
                return;
            }
            $data = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
            ['server' => $server, 'traffic' => $traffic, 'protocol' => $protocol, 'timestamp' => $timestamp] = $data;
            (new TrafficFetchJob($server, $traffic, $protocol, $timestamp))->handle();
            (new StatUserJob($server, $traffic, $protocol))->handle($timestamp);
            (new StatServerJob($server, $traffic, $protocol))->handle($timestamp);
            $pending->delete();
        }, 3);
    }
}
