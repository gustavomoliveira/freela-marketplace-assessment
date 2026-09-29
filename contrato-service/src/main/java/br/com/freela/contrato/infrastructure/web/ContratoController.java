package br.com.freela.contrato.infrastructure.web;

import br.com.freela.contrato.application.ContratoApplicationService;
import br.com.freela.contrato.application.CriarContratoCommand;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/contratos")
public class ContratoController {
    private static final Logger log = LoggerFactory.getLogger(ContratoController.class);
    private final ContratoApplicationService service;
    public ContratoController(ContratoApplicationService service){this.service=service;}

    @PostMapping @ResponseStatus(HttpStatus.CREATED)
    public ContratoResponse criar(@RequestHeader(value="X-Correlation-Id", required=false) String correlationId,
                                  @Valid @RequestBody CriarContratoRequest request) {
        log.info("http.contrato.criar correlationId={} clienteId={} freelancerId={} titulo={}", correlationId, request.clienteId(), request.freelancerId(), request.titulo());
        var c = service.criar(new CriarContratoCommand(request.clienteId(), request.freelancerId(), request.titulo(), request.valor()));
        log.info("http.contrato.criar.response correlationId={} contratoId={} status={}", correlationId, c.id(), c.status());
        return ContratoResponse.from(c);
    }

    @PostMapping("/{id}/entrega")
    public ContratoResponse registrarEntrega(@RequestHeader(value="X-Correlation-Id", required=false) String correlationId,
                                             @PathVariable UUID id) {
        log.info("http.contrato.entrega correlationId={} contratoId={}", correlationId, id);
        var c = service.registrarEntrega(id);
        log.info("http.contrato.entrega.response correlationId={} contratoId={} status={}", correlationId, c.id(), c.status());
        return ContratoResponse.from(c);
    }

    @PostMapping("/{id}/conclusao")
    public ContratoResponse concluir(@RequestHeader(value="X-Correlation-Id", required=false) String correlationId,
                                     @PathVariable UUID id) {
        log.info("http.contrato.conclusao correlationId={} contratoId={}", correlationId, id);
        var c = service.concluir(id);
        log.info("http.contrato.conclusao.response correlationId={} contratoId={} status={}", correlationId, c.id(), c.status());
        return ContratoResponse.from(c);
    }

    @PostMapping("/{id}/cancelamento")
    public ContratoResponse cancelar(@RequestHeader(value="X-Correlation-Id", required=false) String correlationId,
                                     @PathVariable UUID id) {
        log.info("http.contrato.cancelamento correlationId={} contratoId={}", correlationId, id);
        var c = service.cancelar(id);
        log.info("http.contrato.cancelamento.response correlationId={} contratoId={} status={}", correlationId, c.id(), c.status());
        return ContratoResponse.from(c);
    }

    @GetMapping("/{id}")
    public ContratoResponse buscar(@RequestHeader(value="X-Correlation-Id", required=false) String correlationId, @PathVariable UUID id) {
        log.info("http.contrato.buscar correlationId={} contratoId={}", correlationId, id);
        return ContratoResponse.from(service.buscar(id));
    }

    @GetMapping
    public List<ContratoResponse> listar(@RequestHeader(value="X-Correlation-Id", required=false) String correlationId) {
        log.info("http.contrato.listar correlationId={}", correlationId);
        return service.listar().stream().map(ContratoResponse::from).toList();
    }
}