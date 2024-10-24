package org.acme;

import java.time.Instant;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Version;

@Entity
public class MutexOwner extends PanacheEntity {

    @Column(unique = true, nullable = false)
    public String identity;

    public Instant lastCheck;

    @Version
    private int version;

}