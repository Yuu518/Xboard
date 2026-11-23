<?php

namespace Tests\Feature\Server;

use App\Jobs\ApplyTrafficReport;
use App\Models\Server;
use App\Models\ServerMachine;
use App\Models\User;
use App\Services\NodeRegistry;
use App\WebSocket\NodeEventHandlers;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\Bus;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use Illuminate\Support\Str;
use Tests\TestCase;
use Workerman\Connection\TcpConnection;

class ServerHandshakeTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        config()->set('app.key', 'base64:' . base64_encode(str_repeat('a', 32)));
        admin_setting([
            'server_token' => 'server-token',
            'server_ws_enable' => 0,
        ]);
    }

    public function test_v2_handshake_accepts_token_only_without_node(): void
    {
        $response = $this->postJson('/api/v2/server/handshake', [
            'token' => 'server-token',
        ]);

        $response->assertOk()->assertJsonStructure(['websocket' => ['enabled']]);
    }

    public function test_v2_handshake_rejects_invalid_token(): void
    {
        $response = $this->postJson('/api/v2/server/handshake', [
            'token' => 'wrong-token',
        ]);

        $response->assertStatus(422);
    }

    public function test_v2_report_works_without_node_type(): void
    {
        Bus::fake();

        $server = $this->makeServer();

        $response = $this->postJson('/api/v2/server/report', [
            'token' => 'server-token',
            'node_id' => $server->id,
        ]);

        $response->assertOk()->assertJson(['data' => true]);
    }

    public function test_v2_report_ignores_node_type_field(): void
    {
        Bus::fake();

        $server = $this->makeServer();

        // legacy node clients may still send node_type; V2 must accept it as no-op.
        $response = $this->postJson('/api/v2/server/report', [
            'token' => 'server-token',
            'node_id' => $server->id,
            'node_type' => 'this-would-be-rejected-by-v1',
        ]);

        $response->assertOk()->assertJson(['data' => true]);
    }

    public function test_v2_report_rejects_unknown_node(): void
    {
        $response = $this->postJson('/api/v2/server/report', [
            'token' => 'server-token',
            'node_id' => 999999,
        ]);

        $response->assertStatus(400);
        $response->assertJson(['message' => 'Server does not exist']);
    }

    public function test_v2_machine_handshake_with_machine_id_and_no_node(): void
    {
        $machine = ServerMachine::create([
            'name' => 'test-machine',
            'token' => 'machine-token',
            'is_active' => true,
        ]);

        $response = $this->postJson('/api/v2/server/handshake', [
            'machine_id' => $machine->id,
            'token' => 'machine-token',
        ]);

        $response->assertOk();
    }

    public function test_v2_machine_report_requires_node_id(): void
    {
        $machine = ServerMachine::create([
            'name' => 'test-machine',
            'token' => 'machine-token',
            'is_active' => true,
        ]);

        $response = $this->postJson('/api/v2/server/report', [
            'machine_id' => $machine->id,
            'token' => 'machine-token',
        ]);

        $response->assertStatus(422);
    }

    public function test_traffic_report_retry_only_charges_once(): void
    {
        $server = $this->makeServer();
        $user = User::create([
            'email' => 'traffic@example.com',
            'password' => 'test',
            'uuid' => (string) Str::uuid(),
            'token' => Str::random(32),
        ]);
        Redis::shouldReceive('sadd')->twice()->andReturn(1);
        $report = [
            'token' => 'server-token',
            'node_id' => $server->id,
            'report_stream' => (string) Str::uuid(),
            'report_seq' => 1,
            'traffic' => [$user->id => [100, 200]],
        ];
        for ($attempt = 0; $attempt < 2; $attempt++) {
            $this->postJson('/api/v2/server/report', $report)->assertOk()
                ->assertJsonPath('report_stream', $report['report_stream'])
                ->assertJsonPath('report_seq', 1);
        }
        $this->assertEquals(100, $user->refresh()->u);
        $this->assertEquals(200, $server->refresh()->d);
        $report['report_seq'] = 3;
        $this->postJson('/api/v2/server/report', $report)->assertStatus(409);
        $report['report_seq'] = 2;
        $this->postJson('/api/v2/server/report', $report)->assertOk();
        $this->assertEquals(200, $user->refresh()->u);
        $this->assertEquals(400, $server->refresh()->d);
    }

    public function test_pending_report_survives_queue_redelivery(): void
    {
        Bus::fake([ApplyTrafficReport::class]);
        $server = $this->makeServer();
        $report = [
            'token' => 'server-token',
            'node_id' => $server->id,
            'report_stream' => (string) Str::uuid(),
            'report_seq' => 1,
            'traffic' => [1001 => [100, 200]],
        ];
        $this->postJson('/api/v2/server/report', $report)->assertOk();
        $this->postJson('/api/v2/server/report', $report)->assertOk();
        Bus::assertDispatchedTimes(ApplyTrafficReport::class, 1);
        $pending = DB::table('v2_server_traffic_pending');
        $this->assertEquals(1, $pending->count());
        $pending->update(['created_at' => time() - 600]);
        ApplyTrafficReport::retryPending();
        Bus::assertDispatchedTimes(ApplyTrafficReport::class, 2);
        Redis::shouldReceive('sadd')->once()->andReturn(1);
        $job = new ApplyTrafficReport($pending->value('id'));
        $job->handle();
        $job->handle();
        $this->assertEquals(0, $pending->count());
        $this->assertEquals(200, $server->refresh()->d);
    }

    public function test_websocket_notifications_keep_the_node_id_without_sending_user_lists(): void
    {
        $node = $this->makeServer();
        $connection = $this->createMock(TcpConnection::class);
        $connection->syncNotifications = true;
        $events = [];
        $connection->method('send')->willReturnCallback(function ($message) use (&$events) {
            $events[] = json_decode($message);
            return true;
        });
        NodeRegistry::add($node->id, $connection);
        try {
            NodeEventHandlers::pushFullSync($connection, $node);
            $this->assertSame(['sync.config', 'sync.users'], array_column($events, 'event'));
            $this->assertEquals((object) [], $events[1]->data);
            $connection->machineNodeIds = [$node->id];
            NodeRegistry::send($node->id, 'sync.users', ['users' => [['id' => 1]]]);
            $this->assertEquals((object) ['node_id' => $node->id], $events[2]->data);
            $connection->syncNotifications = false;
            NodeRegistry::send($node->id, 'sync.users', ['users' => [['id' => 1]]]);
            $this->assertEquals(1, $events[3]->data->users[0]->id);
        } finally {
            NodeRegistry::remove($node->id, $connection);
        }
    }

    public function test_config_negotiates_sync_policy_and_capabilities_over_http(): void
    {
        $node = $this->makeServer();
        $url = '/api/v2/server/config?' . http_build_query(['token' => 'server-token', 'node_id' => $node->id]);
        $legacy = $this->getJson($url)->assertOk()->json();
        $this->assertTrue($legacy['base_config']['report_ack']);
        unset($legacy['base_config']);

        $url .= '&config_format=2';
        $config = $this->getJson($url)->assertOk()
            ->assertJsonPath('capabilities.report_ack', true)
            ->assertJsonPath('capabilities.report_parts', true);
        $this->assertSame($legacy, $config->json('node_config'));
        $headers = ['If-None-Match' => $config->headers->get('ETag')];
        $this->getJson($url, $headers)->assertStatus(304);
        admin_setting(['server_pull_interval' => 17, 'server_push_interval' => 23]);
        $updated = $this->getJson($url, $headers)->assertOk()
            ->assertJsonPath('sync.pull_interval', 17)
            ->assertJsonPath('sync.push_interval', 23);
        $this->assertSame($legacy, $updated->json('node_config'));

        $machine = ServerMachine::create(['name' => 'test-machine', 'token' => 'machine-token', 'is_active' => true]);
        $credentials = ['machine_id' => $machine->id, 'token' => 'machine-token'];
        $this->postJson('/api/v2/server/machine/nodes', $credentials)->assertOk()
            ->assertJsonPath('base_config.push_interval', 23);
        $this->postJson('/api/v2/server/machine/nodes', $credentials + ['config_format' => 2])->assertOk()
            ->assertJsonPath('sync.pull_interval', 17)->assertJsonMissingPath('base_config');
        $machine->delete();
        $this->postJson('/api/v2/server/machine/nodes', $credentials)->assertForbidden()
            ->assertHeader('X-Xboard-Machine-State', 'unavailable');
    }

    public function test_traffic_and_status_receive_independent_receipts(): void
    {
        Bus::fake([ApplyTrafficReport::class]);
        $node = $this->makeServer();
        $base = ['token' => 'server-token', 'node_id' => $node->id, 'report_parts' => true];
        $batch = ['report_stream' => (string) Str::uuid(), 'report_seq' => 1, 'traffic' => [1001 => [10, 20]]];
        $url = '/api/v2/server/report';
        $this->postJson($url, $base + $batch + ['status' => 'invalid'])->assertOk()
            ->assertJsonPath('results.traffic.ok', true)
            ->assertJsonPath('results.traffic.report_stream', $batch['report_stream'])
            ->assertJsonPath('results.traffic.report_seq', 1)
            ->assertJsonPath('results.status.ok', false)
            ->assertJsonPath('results.status.code', 422);
        $this->postJson($url, $base + ['status' => ['cpu' => 10]])->assertOk()
            ->assertExactJson(['results' => ['status' => ['ok' => true]]]);
        $this->postJson($url, $base + $batch)->assertOk()
            ->assertJsonPath('results.traffic.ok', true)->assertJsonMissingPath('results.status');
        $batch['report_seq'] = 3;
        $this->postJson($url, $base + $batch + ['status' => ['cpu' => 40]])->assertOk()
            ->assertJsonPath('results.traffic.code', 409)->assertJsonPath('results.status.ok', true);
        $this->postJson($url, $base + ['traffic' => [1001 => [1, 2]]])->assertOk()
            ->assertJsonPath('results.traffic.code', 422);
        Bus::assertDispatchedTimes(ApplyTrafficReport::class, 1);
        $this->assertSame(1, DB::table('v2_server_traffic_pending')->count());
    }

    public function test_machine_list_notifications_require_explicit_support(): void
    {
        $connection = $this->createMock(TcpConnection::class);
        $connection->syncNotifications = true;
        $payloads = [];
        $connection->method('send')->willReturnCallback(function ($message) use (&$payloads) {
            $payloads[] = json_decode($message)->data;
            return true;
        });
        $machine = ServerMachine::create(['name' => 'test-machine', 'token' => 'machine-token', 'is_active' => true]);
        $node = $this->makeServer();
        $node->forceFill(['machine_id' => $machine->id])->saveQuietly();
        NodeRegistry::addMachine($machine->id, $connection);
        try {
            NodeEventHandlers::pushMachineNodes($machine->id);
            $this->assertSame($node->id, $payloads[0]->nodes[0]->id);
            $this->assertSame($connection, NodeRegistry::get($node->id));
            $node->forceFill(['machine_id' => null])->saveQuietly();
            $connection->nodeListNotifications = true;
            NodeEventHandlers::pushMachineNodes($machine->id);
            $this->assertNull(NodeRegistry::get($node->id));
            $this->assertEquals((object) [], $payloads[1]);
        } finally {
            NodeRegistry::remove($node->id, $connection);
            NodeRegistry::removeMachine($machine->id, $connection);
        }
    }

    private function makeServer(): Server
    {
        return Server::create([
            'name' => 'test-node',
            'type' => Server::TYPE_VMESS,
            'host' => '127.0.0.1',
            'port' => 443,
            'server_port' => 443,
            'rate' => '1',
            'group_id' => [1],
            'enabled' => true,
        ]);
    }
}
